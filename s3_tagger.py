import pandas as pd
import argparse
import boto3


def read_csv(csv_file):
    table_info = pd.read_csv(csv_file)
    return table_info


def tag_object(key):
    split_string = key.split("/")
    objects_to_tag = split_string[-1]
    table_name = split_string[-2]
    db_name = split_string[-3]
    pii_value = ""

    csv_data = read_csv(args.csv_location)

    for rowindex, row in csv_data.iterrows():
        if row["db"] == db_name and row["table"] == table_name:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Take locations as args")

    parser.add_argument("--csv_location", help="The location of the CSV file to parse")
    parser.add_argument(
        "--path_prefix",
        help="The path to crawl through where objects need to be tagged",
    )
    args = parser.parse_args()

    s3_bucket = data.output  # ${published_bucket}

    prefix_to_tag = args.path_prefix

    s3_client = boto3.client("s3")
    objects_in_prefix = s3_client.list_objects(Bucket=s3_bucket, Prefix=prefix_to_tag)[
        "Contents"
    ]

    objects_to_tag = []
    for key in objects_in_prefix:
        if "$folder$" not in key["Key"]:
            objects_to_tag.append(key["Key"])

    for row in objects_to_tag:
        tag_object(row)
