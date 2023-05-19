from fastapi import FastAPI, UploadFile, Form, File
import boto3, os
from botocore.exceptions import ClientError
from datetime import datetime
from pydantic import BaseModel
import uuid, io
from boto3.dynamodb.conditions import Attr
from pathlib import Path
from dotenv import dotenv_values
from mangum import Mangum

env_vars = dotenv_values('.env')

# Access the variables
region = env_vars['AWS_REGION']
aws_access_key_id = env_vars['AWS_ACCESS_KEY_ID']
aws_secret_access_key = env_vars['AWS_SECRET_ACCESS_KEY']
bucket_name = env_vars['BUCKET_NAME']


# Initialize FastAPI app
app = FastAPI()
handler = Mangum(app)


# Initialize AWS clients with the region and credentials
s3 = boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
dynamodb = boto3.resource('dynamodb', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
sqs = boto3.client('sqs', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)


# Define DynamoDB table name
metadata_table_name = 'file-table'

# Define SQS queue name
processing_queue_name = 'file-queue'


class FileMetadata(BaseModel):
    id: str
    file_name: str
    file_size: int
    upload_timestamp: str
    user: str
    processed_flag: bool


async def upload_file_to_s3(file: UploadFile):
    try:
        file_contents = await file.read()
        # Get the file size
        file_size = len(file_contents)
        file_obj = io.BytesIO(file_contents)
        s3.upload_fileobj(file_obj, 'bucket-1005', file.filename)
        
        return True, file_size
    except ClientError as e:
        # Handle S3 upload error
        print(e)
        return False


def save_file_metadata(metadata: FileMetadata):
    try:
        table = dynamodb.Table(metadata_table_name)
        table.put_item(Item=metadata.dict())
        return True
    except ClientError as e:
        # Handle DynamoDB put item error
        print(e)
        return False


def process_file(file_name: str):
    try:
        # Perform file processing
        # ...

        # Update metadata in DynamoDB
        table = dynamodb.Table(metadata_table_name)
        table.update_item(
            Key={'id': file_name},
            UpdateExpression='SET processed_flag = :processed_flag',
            ExpressionAttributeValues={':processed_flag': True}
        )
        return True
    except Exception as e:
        # Handle file processing error
        print(e)
        return False


def get_file_metadata(file_name: str):
    try:
        table = dynamodb.Table(metadata_table_name)
        response = table.get_item(Key={'id': file_name})
        return response.get('Item', None)
    except ClientError as e:
        # Handle DynamoDB get item error
        print(e)
        return None

@app.get("/ping")
def ping():
    return{"message": "pong"}

@app.get("/")
def ping():
    return{"message": "hey Qblock"}

@app.post('/upload')
async def upload_file(file: UploadFile = File(...), prompt: str = Form(...), user: str = Form(...)):
    # Upload file to S3
    status, file_size = await upload_file_to_s3(file)
    if status:

        metadata = FileMetadata(
            id=file.filename,
            file_name=file.filename,
            file_size=file_size,
            upload_timestamp=datetime.now().isoformat(),
            user=user,
            processed_flag=False
        )
        

        if save_file_metadata(metadata):
            # Send file processing message to SQS queue
            sqs.send_message(
                QueueUrl='https://sqs.us-east-1.amazonaws.com/490295662672/file-queue',
                MessageBody=file.filename
            )
            
            return {'message': 'File uploaded successfully'}
        else:
            return {'message': 'Failed to save file metadata'}
    else:
        return {'message': 'Failed to upload file to S3'}


import os

@app.get('/download/{file_name}')
async def download_file(file_name: str):
    # Get file metadata
    metadata = get_file_metadata(file_name)
    process_file(file_name)
    if metadata:
        if metadata['processed_flag']:
            # Process the file
            # process_file(file_name)

            # Generate pre-signed URL for file download
            try:
                presigned_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': 'bucket-1005', 'Key': file_name},
                    ExpiresIn=3600
                )

                # Download the file
                response = s3.get_object(Bucket='bucket-1005', Key=file_name)
                file_contents = response['Body'].read()
                downloads_folder = '/root/Downloads'
                os.makedirs(downloads_folder, exist_ok=True)

                # Save the file to the downloads folder
                file_path = os.path.join(downloads_folder, file_name)
                # Save the file to the current directory
                with open(file_path, 'wb') as file:
                    file.write(file_contents)

                return {'message': 'File downloaded successfully'}
            except ClientError as e:
                # Handle S3 pre-signed URL generation error
                print(e)
                return {'message': 'Failed to generate pre-signed URL'}
        else:
            return {'message': 'File is still being processed'}
    else:
        return {'message': 'File not found'}

@app.get('/metadata/{file_name}')
async def file_metadata(file_name: str):
    # Get file metadata
    metadata = get_file_metadata(file_name)
    if metadata:
        return metadata
    else:
        return {'message': 'File not found'}

@app.get('/metadata')
async def get_all_metadata():
    # Get all file metadata
    # Fetch all items from the DynamoDB table
    table = dynamodb.Table(metadata_table_name)
    response = table.scan()
    items = response.get('Items', [])
    return items

@app.get('/files')
async def get_all_files():
    # Get a list of all files
    # Fetch all items from the DynamoDB table
    table = dynamodb.Table(metadata_table_name)
    response = table.scan()
    items = response.get('Items', [])
    files = [item['file_name'] for item in items]
    return files

@app.get('/processed-files')
async def get_processed_files():
    # Get a list of processed files
    # Fetch items from the DynamoDB table where 'processed' is True
    table = dynamodb.Table(metadata_table_name)
    response = table.scan(FilterExpression=Attr('processed_flag').eq(True))
    items = response.get('Items', [])
    files = [item['file_name'] for item in items]
    return files

@app.delete('/file/{file_name}')
async def delete_file(file_name: str):
    # Delete a file and its metadata
    # Delete the file from S3 bucket
    try:
        s3.delete_object(Bucket='bucket-1005', Key=file_name)
    except ClientError as e:
        # Handle S3 delete object error
        print(e)
        return {'message': 'Failed to delete file from S3'}

    # Delete the metadata from DynamoDB
    try:
        table = dynamodb.Table(metadata_table_name)
        table.delete_item(Key={'id': file_name})
        return {'message': 'File deleted successfully'}
    except ClientError as e:
        # Handle DynamoDB delete item error
        print(e)
        return {'message': 'Failed to delete file metadata'}


@app.get('/user/{user}/files')
async def get_user_files(user: str):
    # Get a list of files uploaded by a specific user
    # Fetch items from the DynamoDB table where 'user' matches the provided user
    table = dynamodb.Table(metadata_table_name)
    response = table.scan(FilterExpression=Attr('user').eq(user))
    items = response.get('Items', [])
    files = [item['file_name'] for item in items]
    return files

@app.get('/file/{file_name}/size')
async def get_file_size(file_name: str):
    # Get the size of a specific file
    metadata = get_file_metadata(file_name)
    if metadata:
        return {'file_size': metadata['file_size']}
    else:
        return {'message': 'File not found'}


@app.get('/user/{user}/metadata')
async def get_user_metadata(user: str):
    # Get metadata for all files uploaded by a specific user
    table = dynamodb.Table(metadata_table_name)
    response = table.scan(FilterExpression=Attr('user').eq(user))
    items = response.get('Items', [])
    metadata_list = []
    for item in items:
        metadata_list.append({
            'file_name': item['file_name'],
            'file_size': item['file_size'],
            'upload_timestamp': item['upload_timestamp'],
            'processed': item['processed_flag']
        })
    return metadata_list



# Additional error handling, logging, and monitoring can be implemented as required


# Run the FastAPI app
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
