import argparse
import re
import csv
import boto3
import logging
import os
import sys


def setup_logging(log_level, log_path):
    app_logger = logging.getLogger("s3_tagger")
    formatter = "{ 'timestamp': '%(asctime)s', 'name': '%(name)s', 'log_level': '%(levelname)s', 'message': '%(message)s' }"
    for old_handler in app_logger.handlers:
        app_logger.removeHandler(old_handler)
    if log_path is None:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(log_path)
    new_level = logging.getLevelName(log_level.upper())
    handler.setFormatter(logging.Formatter(formatter))
    app_logger.addHandler(handler)
    app_logger.setLevel(new_level)
    return app_logger


def read_csv(csv_location, s3_client):
    csv_dict = {}

    bucket = (re.search("s3://([a-zA-Z0-9-]*)", csv_location)).group(1)
    key = ((re.search("s3://[a-zA-Z0-9-]*(.*)",csv_location)).group(1)).lstrip("/")
    file_name = csv_location.split('/')[-1]

    s3_client.download_file(bucket, key, file_name)

    with open(file_name) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["db"] in csv_dict:
                csv_dict[row["db"]].append({"table": row["table"], "pii": row["pii"]})
            else:
                csv_dict[row["db"]] = [{"table": row["table"], "pii": row["pii"]}]
    return csv_dict


def tag_object(key, s3_client, s3_bucket):
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
        pii_value = " "

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
    return objects_in_prefix


def tag_path(objects_in_prefix, s3_client, s3_bucket):
    objects_to_tag = []
    for key in objects_in_prefix:
        if "$folder$" not in key["Key"]:
            objects_to_tag.append(key["Key"])

    logger.info(f"Found {len(objects_to_tag)} objects to tag in specified path")
    tagged_objects = 0
    for row in objects_to_tag:
        is_tagged = tag_object(row, s3_client, s3_bucket)
        tagged_objects = tagged_objects + is_tagged

    if tagged_objects == 0:
        logger.warning("No objects tagged")
    else:
        logger.info(f"{tagged_objects} objects were tagged")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Take locations as args")
    parser.add_argument("--csv_location", help="The location of the CSV file to parse")
    parser.add_argument(
        "--s3_prefix",
        help="The path to crawl through where objects need to be tagged",
    )  # add slash

    args = parser.parse_args()
    s3_prefix = args.s3_prefix
    if s3_prefix.startswith("/"):
        s3_prefix = s3_prefix.lstrip("/")
    if s3_prefix.endswith("/"):
        s3_prefix = s3_prefix.rstrip("/")

    logger = setup_logging(
        log_level=os.environ["S3_TAGGER_LOG_LEVEL"].upper()
        if "S3_TAGGER_LOG_LEVEL" in os.environ
        else "INFO",
        log_path="test-log.log",  # ${log_path}
    )
    s3 = get_s3()
    csv_data = read_csv(args.csv_location, s3)
    objects_in_prefix = get_objects_in_prefix(
        "${PUBLISH_BUCKET}", s3_prefix, s3
    )
    tag_path(objects_in_prefix, s3, ${PUBLISH_BUCKET}")
