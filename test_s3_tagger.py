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
    objects_in_prefix = [{"Key": "data/db1/_$folder$"}, {"Key": "data/db1/00000_0"}]
    return objects_in_prefix


@pytest.fixture(scope="session")
def csv_data(pytestconfig):
    csv_data = {
        "db1": [{"table": "tab1", "pii": "false"}, {"table": "tab3", "pii": "false"}],
        "db2": [{"table": "tab2", "pii": "true"}],
        "db3": [{"table": "tab3", "pii": ""}, {"table": "tab4", "pii": "true"}],
    }
    return csv_data


@pytest.fixture(scope="session")
def objects_in_prefix(pytestconfig):
    objects_in_prefix = ["data/db1/tab1/00000_0", "data/db1/_$folder$"]
    return objects_in_prefix


@pytest.fixture(scope="session")
def objects_to_tag(pytestconfig):
    objects_to_tag = ["data/db1/tab1/00000_0", "data/db2/tab2/00000_0"]
    return objects_to_tag


@pytest.fixture(scope="session")
def objects_to_tag_partitioned(pytestconfig):
    objects_to_tag = [
        "data/db1/tab1/partition1/00000_0",
        "data/db2/tab2/partition2/00000_0",
    ]
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
def test_tag_objects_threaded(objects_to_tag, csv_data):
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
    for result in s3_tagger.tag_objects_threaded(
        objects_to_tag, s3_client, BUCKET_TO_TAG, csv_data
    ):
        assert result == 1, f"Expecting result to be 1, but got {result}"

    response = s3_client.get_object_tagging(Bucket=BUCKET_TO_TAG, Key=objects_to_tag[1])
    response2 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag[0]
    )
    assert response["TagSet"][2]["Value"] == "true", "Object was not tagged correctly"
    assert response2["TagSet"][2]["Value"] == "false", "Object was not tagged correctly"


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
        f'Table is missing from the CSV data ", "table_name": "tab2", "db_name": "db1", "key": "{key}'
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
def test_tag_object_unrecognised_key_for_database_and_table_name():
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

    call_count = s3_tagger.logger.warning.call_count

    s3_tagger.logger.warning.assert_has_calls(
        [
            mock.call(log)
            for log in [
                f'Couldn\'t establish a valid database and table name for key ", "table_name": "", "db_name": "", "key": "{key}',
            ]
        ]
    )

    assert (
        not call_count > 1
    ), f"Expected logger.warning to only be called twice, called {call_count} times"
    assert tag_result == 0, f"Expected tag_object to return 0, got: {tag_result}"


@mock_s3
def test_tag_object_unrecognised_key_for_database_and_table_name_length_two():
    csv_data = {
        "db1": [{"table": "tab1", "pii": "false"}],
        "db2": [{"table": "tab2", "pii": "false"}],
        "db3": [{"table": "tab3", "pii": "false"}],
    }

    key = "data/e2e_test_file"

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(Body="testcontent", Bucket=BUCKET_TO_TAG, Key=key)

    tag_result = s3_tagger.tag_object(key, s3_client, BUCKET_TO_TAG, csv_data)

    call_count = s3_tagger.logger.warning.call_count

    s3_tagger.logger.warning.assert_has_calls(
        [
            mock.call(log)
            for log in [
                f'Skipping file as it doesn\'t appear to match output pattern", "key": "{key}'
            ]
        ]
    )

    assert (
        not call_count > 1
    ), f"Expected logger.warning to only be called twice, called {call_count} times"
    assert tag_result == 0, f"Expected tag_object to return 0, got: {tag_result}"


@mock_s3
def test_tag_object_unrecognised_key_for_database_and_table_name_length_three():
    csv_data = {
        "db1": [{"table": "tab1", "pii": "false"}],
        "db2": [{"table": "tab2", "pii": "false"}],
        "db3": [{"table": "tab3", "pii": "false"}],
    }

    key = "data/db4/tab4_$folder$"

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(Body="testcontent", Bucket=BUCKET_TO_TAG, Key=key)

    tag_result = s3_tagger.tag_object(key, s3_client, BUCKET_TO_TAG, csv_data)
    error_call_count = s3_tagger.logger.warning.call_count

    s3_tagger.logger.error.assert_has_calls(
        [
            mock.call(log)
            for log in [
                f'Caught exception when attempting to establish database name from key. Not tagging and continuing on", "key": "{key}", "exception": "list index out of range',
            ]
        ]
    )

    assert (
        not error_call_count > 1
    ), f"Expected logger.error to only be called twice, called {error_call_count} times"
    assert tag_result == 0, f"Expected tag_object to return 0, got: {tag_result}"


@mock_s3
def test_tag_object_unrecognised_key_for_database_and_table_name_length_six():
    csv_data = {
        "db1": [{"table": "tab1", "pii": "false"}],
        "db2": [{"table": "tab2", "pii": "false"}],
        "db3": [{"table": "tab3", "pii": "false"}],
    }

    key = "data/db4/tab4/partition1/somerandom2/00000_0"

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(Body="testcontent", Bucket=BUCKET_TO_TAG, Key=key)

    tag_result = s3_tagger.tag_object(key, s3_client, BUCKET_TO_TAG, csv_data)
    call_count = s3_tagger.logger.warning.call_count

    s3_tagger.logger.warning.assert_has_calls(
        [
            mock.call(log)
            for log in [
                f'Couldn\'t establish a valid database and table name for key ", "table_name": "", "db_name": "", "key": "{key}',
            ]
        ]
    )

    assert (
        not call_count > 1
    ), f"Expected logger.warning to only be called twice, called {call_count} times"
    assert tag_result == 0, f"Expected tag_object to return 0, got: {tag_result}"


@mock_s3
def test_dropping_database_trailing_dot_db(csv_data):

    key = "data/db1.db/tab1/0000_0"

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(Body="testcontent", Bucket=BUCKET_TO_TAG, Key=key)

    tag_result = s3_tagger.tag_object(key, s3_client, BUCKET_TO_TAG, csv_data)
    response = s3_client.get_object_tagging(Bucket=BUCKET_TO_TAG, Key=key)

    assert not s3_tagger.logger.warning.called
    assert tag_result == 1, f"Expected tag_object to return 0, got: {tag_result}"
    assert response["TagSet"][0]["Value"] == "db1", "Object was not tagged correctly"
    assert response["TagSet"][1]["Value"] == "tab1", "Object was not tagged correctly"
    assert response["TagSet"][2]["Value"] == "false", "Object was not tagged correctly"


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
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/_$folder$"
    )
    response = s3_tagger.get_objects_in_prefix(BUCKET_TO_TAG, DATA_S3_PREFIX, s3_client)
    assert len(response) == 2, "invalid objects were not filtered out"


@mock_s3
def test_get_objects_in_prefix_when_no_objects_present():
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
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/_$folder$"
    )
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
def test_tag_path_for_partition(objects_to_tag_partitioned, csv_data):
    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db1/tab1/partition1/00000_0"
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key="data/db2/tab2/partition2/00000_0"
    )
    s3_tagger.tag_path(objects_to_tag_partitioned, s3_client, BUCKET_TO_TAG, csv_data)

    response = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag_partitioned[0]
    )
    response2 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag_partitioned[1]
    )
    assert response["TagSet"][2]["Value"] == "false", "Object was not tagged correctly"
    assert response2["TagSet"][2]["Value"] == "true", "Object was not tagged correctly"


@mock_s3
def test_tag_folder(csv_data):
    objects_to_tag = [
        "data/db1/tab1_$folder$",
        "data/db1/tab1/00000_0",
        "data/db2/tab2_$folder$",
        "data/db1/tab3/partition_$folder$",
        "data/db3/tab4/partition/00000_0",
    ]

    s3_tagger.logger = mock.MagicMock()
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=BUCKET_TO_TAG,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key=objects_to_tag[0]
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key=objects_to_tag[1]
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key=objects_to_tag[2]
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key=objects_to_tag[3]
    )
    s3_client.put_object(
        Body="testcontent", Bucket=BUCKET_TO_TAG, Key=objects_to_tag[4]
    )

    s3_tagger.tag_path(objects_to_tag, s3_client, BUCKET_TO_TAG, csv_data)
    response1 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag[0]
    )
    response2 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag[1]
    )
    response3 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag[2]
    )
    response4 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag[3]
    )
    response5 = s3_client.get_object_tagging(
        Bucket=BUCKET_TO_TAG, Key=objects_to_tag[4]
    )

    assert response1["TagSet"][2]["Value"] == "false", "Object was not tagged correctly"
    assert response2["TagSet"][2]["Value"] == "false", "Object was not tagged correctly"
    assert response3["TagSet"][2]["Value"] == "true", "Object was not tagged correctly"
    assert response4["TagSet"][2]["Value"] == "false", "Object was not tagged correctly"
    assert response5["TagSet"][2]["Value"] == "true", "Object was not tagged correctly"


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
    assert len(response) == 2, "invalid objects were not filtered out"
