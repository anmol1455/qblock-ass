from pydantic import BaseSettings

class Settings(BaseSettings):
    AWS_REGION=str
    AWS_ACCESS_KEY_ID=str
    AWS_SECRET_ACCESS_KEY=str
    BUCKET_NAME=str

config = Settings(".env")