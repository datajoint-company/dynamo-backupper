FROM python:slim-buster

RUN pip3 install pandas boto3 google-api-python-client google-auth-httplib2 --no-cache-dir

COPY ./dynamo_backupper /src/dynamo_backupper
COPY ./setup.py /src
COPY ./scripts /src/scripts
RUN pip3 install /src

WORKDIR /src

CMD ["python3", "./scripts/main.py"]