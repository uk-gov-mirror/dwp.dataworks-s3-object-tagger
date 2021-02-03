FROM python:3.8-alpine3.10

WORKDIR /usr/src/app

RUN apk --update --no-cache add gcc musl-dev libffi-dev openssl-dev

COPY requirements.txt ./
RUN  pip install -r requirements.txt

COPY s3_tagger.py ./

ENTRYPOINT ["python", "s3_tagger.py"]

