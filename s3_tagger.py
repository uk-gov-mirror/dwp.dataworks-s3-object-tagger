import pandas as pd
import argparse
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


def read_csv(csv_file):
    table_info = pd.read_csv(csv_file)
    return table_info


def tag_object(key, s3_client, s3_bucket):
    split_string = key.split("/")
    objects_to_tag = split_string[-1]
    table_name = split_string[-2]
    db_name = split_string[-3]
    pii_value = ""
    tag_info_found = None
    csv_data = read_csv(args.csv_location)

    for rowindex, row in csv_data.iterrows():
        if row["db"] == db_name and row["table"] == table_name:
            tag_info_found = True
            pii_value = row["pii"]

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


def get_objects_in_prefix(s3_bucket, prefix_to_tag, s3_client):

    prefix_to_tag = args.path_prefix
    objects_in_prefix = s3_client.list_objects(Bucket=s3_bucket, Prefix=prefix_to_tag)[
        "Contents"
    ]
    return objects_in_prefix


def tag_path(objects_in_prefix, s3_client, s3_bucket):
    objects_to_tag = []
    for key in objects_in_prefix:
        if "$folder$" not in key["Key"]:
            objects_to_tag.append(key["Key"])

    logger.info("Found {} objects to tag in specified path".format(len(objects_to_tag)))
    tagged_objects = 0
    for row in objects_to_tag:
        is_tagged = tag_object(row, s3_client, s3_bucket)
        tagged_objects = tagged_objects + is_tagged
    logger.info("{} objects were tagged".format(tagged_objects))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Take locations as args")
    parser.add_argument("--csv_location", help="The location of the CSV file to parse")
    parser.add_argument(
        "--path_prefix",
        help="The path to crawl through where objects need to be tagged",
    )  # add slash
    args = parser.parse_args()
    logger = setup_logging(
        log_level=os.environ["S3_TAGGER_LOG_LEVEL"].upper()
        if "S3_TAGGER_LOG_LEVEL" in os.environ
        else "INFO",
        log_path="${log_path}",  # ${log_path}
    )
    s3 = get_s3()
    objects_in_prefix = get_objects_in_prefix("s3bucket", args.path_prefix, s3)
    tag_path(objects_in_prefix, s3, "s3bucket")
