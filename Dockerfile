FROM python:3.10-slim


WORKDIR /qblock

COPY ./requirements.txt /qblock/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . /qblock/

EXPOSE 8000

ENTRYPOINT [ "bash" , "entrypoint.sh"]