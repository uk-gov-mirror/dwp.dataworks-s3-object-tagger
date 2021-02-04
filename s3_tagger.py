import argparse
import csv
import logging
import os
import re
import socket
import sys

import boto3


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
        f'Downloading CSV",  "Bucket": "{bucket}", "Key": "{key}", "file_name": "{file_name}'
    )

    try:
        s3_client.download_file(bucket, key, file_name)
        logger.info(f'Successfully downloaded", "file_name": "{file_name}')

        logger.info(f'Attempting to read into dictionary", "CSV": "{file_name}')
        with open(file_name) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["db"] in csv_dict:
                    csv_dict[row["db"]].append(
                        {"table": row["table"], "pii": row["pii"]}
                    )
                else:
                    csv_dict[row["db"]] = [{"table": row["table"], "pii": row["pii"]}]
        logger.info(f'Successfully read", "CSV": "{file_name}')
        return csv_dict
    except Exception as err:
        logger.error(
            f'Failed to Download/Read", "CSV": "{file_name}", "error_message": "{err}'
        )
        sys.exit(-1)


def tag_object(key, s3_client, s3_bucket, csv_data):
    split_string = key.split("/")
    table_name = split_string[-2]
    db_name = split_string[-3]
    pii_value = ""
    tag_info_found = None

    if db_name in csv_data:
        for table in csv_data[db_name]:
            if table_name == table["table"]:
                pii_value = table["pii"]
                tag_info_found = True

    if type(pii_value) != str:
        pii_value = ""

    if pii_value == "":
        logger.warning(
            f'No PII classification found for", "table_name": "{table_name}", "db_name": "{db_name}'
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
        logger.info(f'Getting list of objects in", "s3_prefix": "{s3_prefix}')
        objects_in_prefix = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_prefix)[
            "Contents"
        ]
        objects_to_tag = []
        # Filter out temp folders that do not hold any objects for tagging
        for key in objects_in_prefix:
            if "$folder$" not in key["Key"]:
                objects_to_tag.append(key["Key"])

        return objects_to_tag

    except Exception as err:
        logger.error(
            f'Failed to list objects", "bucket": "{s3_bucket}", "s3_prefix": "{s3_prefix}", "error_message":"{err}'
        )


def tag_path(objects_to_tag, s3_client, s3_bucket, csv_data):
    logger.info(f'Found objects to tag", "number_of_objects": "{len(objects_to_tag)}')
    tagged_objects_count = 0
    for row in objects_to_tag:
        is_tagged = tag_object(row, s3_client, s3_bucket, csv_data)
        tagged_objects_count = tagged_objects_count + is_tagged

    if tagged_objects_count == 0:
        logger.info(
            f'Did not tag any objects", "number_of_objects": "{len(tagged_objects_count)}'
        )
    else:
        logger.info(f'Tagged", "objects_tagged_count": "{tagged_objects_count}')


def get_parameters():
    parser = argparse.ArgumentParser(
        description="A Python script which receives six args:"
        "1. csv_location"
        "2. bucket"
        "3. s3-prefix"
        "4. log-level"
        "5. environment"
        "6. application"
        "It will then tag all the objects in the prefix as described in the CSV supplied"
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--csv_location", help="The location of the CSV file to parse")
    parser.add_argument("--bucket", help="The bucket to tag")
    parser.add_argument(
        "--s3_prefix", help="The path to crawl through where objects need to be tagged"
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--environment", default="NOT_SET")
    parser.add_argument("--application", default="NOT_SET")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "csv_location" in os.environ:
        _args.csv_location = os.environ["csv_location"]

    if "bucket" in os.environ:
        _args.bucket = os.environ["bucket"]

    if "s3_prefix" in os.environ:
        _args.s3_prefix = os.environ["s3_prefix"]

    if "log_level" in os.environ:
        _args.log_level = os.environ["log_level"]

    if "environment" in os.environ:
        _args.log_level = os.environ["environment"]

    if "application" in os.environ:
        _args.log_level = os.environ["application"]

    required_args = ["csv_location", "bucket", "s3_prefix"]
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
        logger.info("args initiated")

        logger.info("Instantiating S3 client")
        s3 = get_s3()
        logger.info("S3 client instantiated")

        logger.info(
            f'Fetching and reading CSV file", "csv_location": "{args.csv_location}'
        )
        csv_data = read_csv(args.csv_location, s3)
        logger.info(
            f'Getting list of objects to tag in", "bucket": "{args.bucket}", "s3_prefix": "{args.s3_prefix}'
        )
        objects_to_tag = get_objects_in_prefix(args.bucket, args.s3_prefix, s3)
        logger.info(f'Beginning to tag objects", "bucket": "{args.bucket}')
        tag_path(objects_to_tag, s3, args.bucket, csv_data)
        logger.info("--Finished--")
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": "{err}')