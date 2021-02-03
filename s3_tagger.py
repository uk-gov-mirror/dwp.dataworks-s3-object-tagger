import argparse
import re
import csv
import boto3
import logging
import os
import sys


def setup_logging(log_level):
    app_logger = logging.getLogger("s3_tagger")
    formatter = "{ 'timestamp': '%(asctime)s', 'name': '%(name)s', 'log_level': '%(levelname)s', 'message': '%(message)s' }"
    handler = logging.StreamHandler(sys.stdout)
    new_level = logging.getLevelName(log_level.upper())
    handler.setFormatter(logging.Formatter(formatter))
    app_logger.addHandler(handler)
    app_logger.setLevel(new_level)
    return app_logger


logger = setup_logging(
    log_level=os.environ["S3_TAGGER_LOG_LEVEL"].upper()
    if "S3_TAGGER_LOG_LEVEL" in os.environ
    else "INFO"
)


def read_csv(csv_location, s3_client):
    csv_dict = {}

    bucket = (re.search("s3://([a-zA-Z0-9-]*)", csv_location)).group(1)
    key = ((re.search("s3://[a-zA-Z0-9-]*(.*)", csv_location)).group(1)).lstrip("/")
    file_name = csv_location.split("/")[-1]

    try:
        s3_client.download_file(bucket, key, file_name)

        with open(file_name) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["db"] in csv_dict:
                    csv_dict[row["db"]].append(
                        {"table": row["table"], "pii": row["pii"]}
                    )
                else:
                    csv_dict[row["db"]] = [{"table": row["table"], "pii": row["pii"]}]
        return csv_dict
    except Exception as ex:
        logger.error(ex)
        sys.exit(-1)


def tag_object(key, s3_client, s3_bucket, csv_data):
    split_string = key.split("/")
    objects_to_tag = split_string[-1]
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
        logger.warning(f"{table_name} from {db_name} does not have a PII classification")

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

    if tag_info_found:
        return 1
    else:
        return 0


def get_s3():
    s3_client = boto3.client("s3")
    return s3_client


def get_objects_in_prefix(s3_bucket, s3_prefix, s3_client):
    objects_in_prefix = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_prefix)[
        "Contents"
    ]
    objects_to_tag = []
    for key in objects_in_prefix:
        if "$folder$" not in key["Key"]:
            objects_to_tag.append(key["Key"])
    return objects_to_tag


def tag_path(objects_to_tag, s3_client, s3_bucket, csv_data):

    logger.info(f"Found {len(objects_to_tag)} objects to tag in specified path")
    tagged_objects = 0
    for row in objects_to_tag:
        is_tagged = tag_object(row, s3_client, s3_bucket, csv_data)
        tagged_objects = tagged_objects + is_tagged

    if tagged_objects == 0:
        logger.warning("No objects tagged")
    else:
        logger.info(f"{tagged_objects} objects were tagged")

def get_parameters():
    parser = argparse.ArgumentParser(
    description="A Python script which receives three args. 1. CSV location. 2. S3 Bucket. 3. S3-prefix, "
    "It will then tag all the objects in the prefix as described in the CSV supplied"
)

    # Parse command line inputs and set defaults
    parser.add_argument("--csv_location", help="The location of the CSV file to parse")
    parser.add_argument("--bucket", help="The bucket to tag")
    parser.add_argument("--s3_prefix", help="The path to crawl through where objects need to be tagged")  
    parser.add_argument("--log-level", default="INFO")
    


    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "csv_location" in os.environ:
        _args.aws_profile = os.environ["csv_location"]

    if "bucket" in os.environ:
        _args.aws_region = os.environ["bucket"]

    if "s3_prefix" in os.environ:
        _args.api_region = os.environ["s3_prefix"]

    if "log_level" in os.environ:
        _args.v1_kms_region = os.environ["log_level"]

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
    args = get_parameters()

    #remove tailing or leading / from prefix 
    s3_prefix = args.s3_prefix
    if s3_prefix.startswith("/"):
        s3_prefix = s3_prefix.lstrip("/")
    if s3_prefix.endswith("/"):
        s3_prefix = s3_prefix.rstrip("/")

    s3 = get_s3()
    csv_data = read_csv(args.csv_location, s3)
    objects_to_tag = get_objects_in_prefix(args.bucket, s3_prefix, s3)
    tag_path(objects_to_tag, s3, args.bucket, csv_data)
