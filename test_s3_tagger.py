import warnings
from unittest import mock

import boto3
import pytest
from moto import mock_s3

import s3_tagger

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)

TABLE_INFO_BUCKET = "tab-info-bucket"
CSV_LOCATION = "s3://tab-info-bucket/table/info/path/table_info.csv"
DATA_S3_PREFIX = "/data/db1/"
BUCKET_TO_TAG = "buckettotag"


@pytest.fixture(scope="session")
def s3_objects_with_temp_files(pytestconfig):
    objects_in_prefix = [{"Key": "data/db1/$folder$"}, {"Key": "data/db1/00000_0"}]
    return objects_in_prefix


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
def test_read_csv_exception():
    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")

    with pytest.raises(SystemExit):
        output = s3_tagger.read_csv(CSV_LOCATION, s3_client)


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
def test_tag_object_invalid_pii():
    csv_data = {
        "db1": [{"table": "tab1", "pii": 1}],
    }

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

    pii_value = response["TagSet"][-1]["Value"]

    assert (
        pii_value == "None"
    ), f"Expected pii_value to be None (str), got: {pii_value}, type: {type(pii_value)}"


@mock_s3
def test_tag_object_tag_info_not_found():
    csv_data = {
        "db1": [{"table": "tab1", "pii": "false"}],
        "db2": [{"table": "tab1", "pii": "false"}],
        "db3": [{"table": "tab3", "pii": "false"}],
    }

    key = "data/db1/tab2/00000_0"

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(Body="testcontent", Bucket=BUCKET_TO_TAG, Key=key)

    tag_result = s3_tagger.tag_object(key, s3_client, BUCKET_TO_TAG, csv_data)
    response = s3_client.get_object_tagging(Bucket=BUCKET_TO_TAG, Key=key)

    pii_value = response["TagSet"][-1]["Value"]
    call_count = s3_tagger.logger.warning.call_count

    s3_tagger.logger.warning.assert_called_once_with(
        'Table is missing from the CSV data ", "table_name": "tab2", "db_name": "db1'
    )

    s3_tagger.logger.info.assert_called_once_with(
        f'Successfully tagged", "object": "{key}'
    )

    assert (
        not call_count > 2
    ), f"Expected logger.warning to only be called twice, called {call_count} times"
    assert tag_result == 0, f"Expected tag_object to return 0, got: {tag_result}"
    assert pii_value == "None", f"Expected pii_value to be 'None', got: {pii_value}"


@mock_s3
def test_tag_object_missing_database_and_table_name():
    csv_data = {
        "db1": [{"table": "tab1", "pii": "false"}],
        "db2": [{"table": "tab2", "pii": "false"}],
        "db3": [{"table": "tab3", "pii": "false"}],
    }

    key = "data/db4/tab4/00000_0"

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(Body="testcontent", Bucket=BUCKET_TO_TAG, Key=key)

    tag_result = s3_tagger.tag_object(key, s3_client, BUCKET_TO_TAG, csv_data)
    response = s3_client.get_object_tagging(Bucket=BUCKET_TO_TAG, Key=key)

    pii_value = response["TagSet"][-1]["Value"]
    call_count = s3_tagger.logger.warning.call_count

    s3_tagger.logger.warning.assert_has_calls(
        [
            mock.call(log)
            for log in [
                'Database is missing from the CSV data ", "table_name": "tab4", "db_name": "db4',
                'Table is missing from the CSV data ", "table_name": "tab4", "db_name": "db4',
            ]
        ]
    )
    s3_tagger.logger.info.assert_called_once_with(
        f'Successfully tagged", "object": "{key}'
    )

    assert (
        not call_count > 2
    ), f"Expected logger.warning to only be called twice, called {call_count} times"
    assert tag_result == 0, f"Expected tag_object to return 0, got: {tag_result}"
    assert pii_value == "None", f"Expected pii_value to be 'None', got: {pii_value}"


@mock_s3
def test_tag_object_exception():
    csv_data = {"db1": [{"table": "tab1", "pii": "false"}]}

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/tab1/00000_0"
    )
    s3_tagger.tag_object("data/db/tab1/00000_0", s3_client, BUCKET_TO_TAG, csv_data)
    response = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key="data/db1/tab1/00000_0"
    )

    assert len(response["TagSet"]) == 0


@mock_s3
def test_get_objects_in_prefix():
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
def test_get_objects_in_prefix_exception():
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
    with pytest.raises(Exception):
        response = s3_tagger.get_objects_in_prefix(BUCKET_TO_TAG, "abcd", s3_client)


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


@mock_s3
def test_tag_path_no_objects_tagged(csv_data):
    objects_to_tag = ["data/db1/tab1/00000_0", "data/db2/tab2/00000_0"]

    s3_tagger.tag_object = mock.MagicMock()
    s3_tagger.tag_object.return_value = 0

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

    s3_tagger.logger.info.assert_has_calls(
        [
            mock.call(log)
            for log in [
                'Found objects to tag", "number_of_objects": "2',
                'Did not tag any objects", "number_of_objects": "0',
            ]
        ]
    )

    assert len(response["TagSet"]) == 0
    assert len(response2["TagSet"]) == 0


def test_filter_temp_files(s3_objects_with_temp_files):
    s3_tagger.logger = mock.MagicMock()
    response = s3_tagger.filter_temp_files(s3_objects_with_temp_files)
    assert len(response) == 1, "invalid objects were not filtered out"
