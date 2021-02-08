import boto3
import pytest
import warnings
from moto import mock_s3
from unittest import mock

import s3_tagger

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import imp

TABLE_INFO_BUCKET = "tab-info-bucket"
CSV_LOCATION = "s3://tab-info-bucket/table/info/path/table_info.csv"
DATA_S3_PREFIX = "data/db1"
BUCKET_TO_TAG = "buckettotag"


@pytest.fixture(scope="session")
def csv_data(pytestconfig):
    csv_data = {
        "db1": [{"table": "tab1", "pii": "false"}],
        "db2": [{"table": "tab2", "pii": "true"}],
        "db3": [{"table": "tab3", "pii": ""}],
    }
    return csv_data


@pytest.fixture(scope="session")
def objects_in_prefix(pytestconfig):
    objects_in_prefix = ["data/db1/tab1/00000_0", "data/db1/$folder$"]
    return objects_in_prefix


@pytest.fixture(scope="session")
def objects_to_tag(pytestconfig):
    objects_to_tag = ["data/db1/tab1/00000_0", "data/db2/tab2/00000_0"]
    return objects_to_tag


@mock_s3
def test_read_csv():
    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
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
    assert type(output) == dict, "failed reading csv into dictionary"
    assert output["db1"][0]["pii"] == "false", "incorrect dictionary structure"


@mock_s3
def test_tag_object(csv_data):
    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/tab1/00000_0"
    )
    s3_tagger.tag_object("data/db1/tab1/00000_0", s3_client, BUCKET_TO_TAG, csv_data)
    response = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key="data/db1/tab1/00000_0"
    )
    assert response["TagSet"][2]["Value"] == "false", "incorrect pii value"


@mock_s3
def test_get_objects_in_prexif():
    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-east-1"},
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/tab1/00000_0"
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/$folder$"
    )
    response = s3_tagger.get_objects_in_prefix(BUCKET_TO_TAG, DATA_S3_PREFIX, s3_client)
    assert len(response) == 1, "invalid objects were not filtered out"


@mock_s3
def test_tag_path(objects_to_tag, csv_data):
    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/tab1/00000_0"
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db2/tab2/00000_0"
    )
    s3_tagger.tag_path(objects_to_tag, s3_client, BUCKET_TO_TAG, csv_data)
    response = s3_client.get_object_tagging(Bucket=BUCKET_TO_TAG, Key=objects_to_tag[1])
    response2 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag[0]
    )
    assert response["TagSet"][2]["Value"] == "true", "Object was not tagged correctly"
    assert response2["TagSet"][2]["Value"] == "false", "Object was not tagged correctly"
