import argparse
import csv
import logging
import os
import re
import socket
import sys
import boto3

from concurrent.futures import ThreadPoolExecutor, wait

NAME_KEY = "Key"


def setup_logging(logger_level):
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)

    hostname = socket.gethostname()

    json_format = (
        '{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"environment": "{args.environment}", "application": "{args.application}", '
        f'"module": "%(module)s", "process": "%(process)s", '
        f'"thread": "[%(thread)s]", "hostname": "{hostname}" }} '
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level.upper())
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def read_csv(csv_location, s3_client):
    csv_dict = {}
    bucket = (re.search("s3://([a-zA-Z0-9-]*)", csv_location)).group(1)
    key = ((re.search("s3://[a-zA-Z0-9-]*(.*)", csv_location)).group(1)).lstrip("/")
    file_name = csv_location.split("/")[-1]

    logger.info(
        f'Downloading {file_name} CSV",  "csv_bucket": "{bucket}", "csv_key": "{key}", "csv_file_name": "{file_name}'
    )

    try:
        s3_client.download_file(bucket, key, file_name)
        logger.info(f'Successfully downloaded", "file_name": "{file_name}')

        logger.info(
            f'Attempting to read into dictionary", "csv_location": "{csv_location}", "csv_file_name": "{file_name}'
        )
        with open(file_name) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["db"] in csv_dict:
                    csv_dict[row["db"]].append(
                        {"table": row["table"], "pii": row["pii"]}
                    )
                else:
                    csv_dict[row["db"]] = [{"table": row["table"], "pii": row["pii"]}]
        logger.info(
            f'Successfully read", "csv_location": "{csv_location}", "csv_file_name": "{file_name}'
        )
        logger.info(f'CSV read into dictionary as", "csv_dict": "{csv_dict}')
        return csv_dict
    except Exception as err:
        logger.error(
            f'Failed to download or read", "csv_location": "{csv_location}", "csv_file_name": "{file_name}", "error_message": "{err}'
        )
        sys.exit(-1)


def tag_object(key, s3_client, s3_bucket, csv_data):
    split_string = key.split("/")
    table_name = split_string[-2]
    db_name = split_string[-3]
    if db_name.endswith(".db"):
        db_name = db_name.replace(".db", "")
    pii_value = ""
    tag_info_found = False
    db_missing_from_csv = False

    if db_name in csv_data:
        for table in csv_data[db_name]:
            if table_name == table["table"]:
                pii_value = table["pii"]
                tag_info_found = True
    else:
        db_missing_from_csv = True

    if type(pii_value) != str:
        pii_value = ""

    if db_missing_from_csv:
        logger.warning(
            f'Database is missing from the CSV data ", "table_name": "{table_name}", "db_name": "{db_name}'
        )

    if not tag_info_found:
        logger.warning(
            f'Table is missing from the CSV data ", "table_name": "{table_name}", "db_name": "{db_name}'
        )

    elif pii_value == "":
        logger.info(
            f'No PII value as the table has yet to be classified ", "table_name": "{table_name}", "db_name": "{db_name}'
        )

    try:
        s3_client.put_object_tagging(
            Bucket=s3_bucket,
            Key=key,
            Tagging={
                "TagSet": [
                    {"Key": "db", "Value": db_name},
                    {"Key": "table", "Value": table_name},
                    {"Key": "pii", "Value": pii_value},
                ]
            },
        )
        logger.info(f'Successfully tagged", "object": "{key}')
    except Exception as err:
        logger.error(f'Failed to tag", "object": "{key}", "error_message": "{err}')

    if tag_info_found:
        return 1
    else:
        return 0


def get_s3():
    try:
        s3_client = boto3.client("s3")
        return s3_client
    except Exception as err:
        logger.error(f'Failed to create an S3 client", "error_message": "{err}')
        sys.exit(-1)


def get_objects_in_prefix(s3_bucket, s3_prefix, s3_client):
    # remove tailing or leading / from prefix
    if s3_prefix.startswith("/"):
        s3_prefix = s3_prefix.lstrip("/")
    if s3_prefix.endswith("/"):
        s3_prefix = s3_prefix.rstrip("/")

    try:
        logger.info(
            f'Contacting S3 for a list of objects", "data_bucket": "{s3_bucket}", "data_s3_prefix": "{s3_prefix}'
        )

        objects_in_prefix = []
        paginator = s3_client.get_paginator("list_objects_v2")

        page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

        for page in page_iterator:
            objects_in_prefix.extend(page["Contents"])

        logger.info(f'Number of "objects_returned": "{len(objects_in_prefix)}')

        return filter_temp_files(objects_in_prefix)

    except Exception as err:
        logger.error(
            f'Failed to list objects", "data_bucket": "{s3_bucket}", "data_s3_prefix": "{s3_prefix}", "error_message":"{err}'
        )
        raise err


def filter_temp_files(objects_in_prefix):
    """
    Filter out temp folders that do not hold any objects for tagging
    :param objects_in_prefix: list of all objects in the prefix
    :return: list of objects without temp files
    """
    objects_to_tag = []
    for object in objects_in_prefix:
        if "$folder$" not in object[NAME_KEY]:
            objects_to_tag.append(object[NAME_KEY])
    logger.info(f'Number of after filter "objects_filtered": "{len(objects_in_prefix)}')
    return objects_to_tag


def tag_path(objects_to_tag, s3_client, s3_bucket, csv_data):
    logger.info(f'Found objects to tag", "number_of_objects": "{len(objects_to_tag)}')
    tagged_objects_count = 0

    for result in tag_objects_threaded(objects_to_tag, s3_client, s3_bucket, csv_data):
        tagged_objects_count = tagged_objects_count + result

    if tagged_objects_count == 0:
        logger.info(
            f'Did not tag any objects", "number_of_objects": "{tagged_objects_count}'
        )
    else:
        logger.info(f'Tagged", "objects_tagged_count": "{tagged_objects_count}')


def tag_objects_threaded(objects_to_tag, s3_client, s3_bucket, csv_data):
    with ThreadPoolExecutor() as executor:
        future_results = []

        for row in objects_to_tag:
            future_results.append(
                executor.submit(tag_object, row, s3_client, s3_bucket, csv_data)
            )

        wait(future_results)
        for future in future_results:
            try:
                yield future.result()
            except Exception as ex:
                raise AssertionError(ex)


def get_parameters():
    parser = argparse.ArgumentParser(
        description="A Python script which receives six args:"
        "1. csv_location"
        "2. data-bucket"
        "3. data-s3-prefix"
        "4. log-level"
        "5. environment"
        "6. application"
        "It will then tag all the objects in the prefix as described in the CSV supplied"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--csv-location", help="The location of the CSV file to parse")
    parser.add_argument("--data-bucket", help="The bucket to tag")
    parser.add_argument(
        "--data-s3-prefix",
        help="The path to crawl through where objects need to be tagged",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--environment", default="NOT_SET")
    parser.add_argument("--application", default="NOT_SET")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "CSV_LOCATION" in os.environ:
        _args.csv_location = os.environ["CSV_LOCATION"]

    if "DATA_BUCKET" in os.environ:
        _args.data_bucket = os.environ["DATA_BUCKET"]

    if "DATA_S3_PREFIX" in os.environ:
        _args.data_s3_prefix = os.environ["DATA_S3_PREFIX"]

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    if "ENVIRONMENT" in os.environ:
        _args.environment = os.environ["ENVIRONMENT"]

    if "APPLICATION" in os.environ:
        _args.application = os.environ["APPLICATION"]

    required_args = ["csv_location", "data_bucket", "data_s3_prefix"]
    missing_args = []

    for required_message_key in required_args:
        if required_message_key not in _args:
            missing_args.append(required_message_key)

    if missing_args:
        raise argparse.ArgumentError(
            None,
            "ArgumentError: The following required arguments are missing: {}".format(
                ", ".join(missing_args)
            ),
        )

    return _args


if __name__ == "__main__":
    try:
        args = get_parameters()
        logger = setup_logging(args.log_level)

        logger.info(
            f'Args initiated", "csv_location": "{args.csv_location}", "data_bucket": "{args.data_bucket}", '
            f'"data_s3_prefix": "{args.data_s3_prefix}'
        )

        logger.info("Instantiating S3 client")
        s3 = get_s3()
        logger.info("S3 client instantiated")

        logger.info(
            f'Fetching and reading CSV file", "csv_location": "{args.csv_location}'
        )
        csv_data = read_csv(args.csv_location, s3)

        logger.info(
            f'Getting list of objects to tag", "data_bucket": "{args.data_bucket}", '
            f'"data_s3_prefix": "{args.data_s3_prefix}'
        )
        objects_to_tag = get_objects_in_prefix(
            args.data_bucket, args.data_s3_prefix, s3
        )

        logger.info(
            f'Beginning to tag objects", "data_bucket": "{args.data_bucket}", '
            f'"csv_location": "{args.csv_location}'
        )

        logger.debug(
            f'Verbose list of items found and will attempt to tag", "data_bucket": "{args.data_bucket}",'
            f'"objects_to_tag": "{objects_to_tag}'
        )
        tag_path(objects_to_tag, s3, args.data_bucket, csv_data)

        logger.info(
            f'Finished tagging objects", "data_bucket": "{args.data_bucket}, '
            f'"data_s3_prefix": "{args.data_s3_prefix}"'
            f'"csv_location": "{args.csv_location}'
        )

    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": "{err}')
        raise err
