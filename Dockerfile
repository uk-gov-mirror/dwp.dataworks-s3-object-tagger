FROM python:3.8-alpine3.10

workdir /usr/src/app

COPY requirements.txt ./
RUN  pip install -r requirements.txt

COPY s3_tagger.py ./

# CMD ["python", "s3_tagger.py"]

