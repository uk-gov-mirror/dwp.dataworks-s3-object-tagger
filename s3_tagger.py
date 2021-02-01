import csv
import argparse
import boto3

def read_csv(csv_file):
    with open(csv_file) as csv_file:
        data = csv.reader(csv_file, delimiter=',')
        line_count = 0
        return data
        # for row in csv_reader:
        #
        #     if line_count == 0:
        #         print(f'Column names are {", ".join(row)}')
        #         line_count += 1
        #     else:
        #         print(f'\t{row[0]} works in the {row[1]} department, and was born in {row[2]}.')
        #         line_count += 1
        # print(f'Processed {line_count} lines.')


def tag_object(key):
    split_string = key.split("/")
    objects_to_tag = split_string[-1]
    table_name = split_string[-2]
    db_name = split_string[-3]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Take locations as args')

    parser.add_argument("--csv_location", help="The location of the CSV file to parse")
    parser.add_argument("--path_prefix", help="The path to crawl through where objects need to be tagged")
    args = parser.parse_args()

    s3_bucket = "/Users/levankirvalidze/IdeaProjects/dataworks-s3-object-tagger" #${published_bucket}

    csv_tables_info = read_csv(args.csv_location)
    print(csv_tables_info)
    prefix_to_tag = args.path_prefix

    s3_client = boto3.client('s3')
    objects_in_prefix = s3_client.list_objects(Bucket=s3_bucket, Prefix=prefix_to_tag)["Contents"]

    objects_to_tag = []
    for key in objects_in_prefix:
        if "$folder$" not in key["Key"]:
            objects_to_tag.append(key)

    for key in objects_to_tag:
        tag_object(key["Key"])
        """check if db in csv, check if table in csv. attempt to tag it"""








