import s3_tagger
from moto import mock_s3
import csv
import boto3
import logging
import pytest
import argparse
import os

TABLE_INFO_BUCKET = "tab-info-bucket"
CSV_LOCATION = "s3://tab-info-bucket/table/info/path/table_info.csv"
S3_PREFIX = ""
BUCKET_TO_TAG = "buckettotag"
csv_data = {
    "db1": [{"table": "tab1", "pii": "false"}],
    "db2": [{"table": "tab2", "pii": "true"}],
}


@mock_s3
def test_read_csv():
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    s3_client.create_bucket(
        Bucket=TABLE_INFO_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.upload_file(
        "test_data.csv",
        TABLE_INFO_BUCKET,
        "table/info/path/table_info.csv",
    )
    output = s3_tagger.read_csv(CSV_LOCATION, s3_client)
    assert type(output) == dict, ""
    assert output["db1"][0]["pii"] == "false", "not passed"


# @mock_s3
# def test_tag_object():
#     s3_client = boto3.client("s3")
#     s3_client.create_bucket(
#         Bucket=BUCKET_TO_TAG,
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
#     )
#     s3_client.put_object(Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/tab1")
#     s3_tagger.tag_object("data/db1/tab1", s3_client, BUCKET_TO_TAG)
#     response = s3_client.get_object_tagging(Bucket=BUCKET_TO_TAG, Key="data/db1/tab1")
#     assert response["TagSet"][0]["pii"] == "false"
