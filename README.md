# dataworks-s3-object-tagger

## An application to tag S3 objects based on various rules and source config files

This application takes four positional arguments.

1. `--csv-location` This is the full path to the CSV that will be used as a lookup table for the application.

    |db |table|pii  |
    |---|-----|-----|
    |db1|tab1 |false|
    |db2|tab2 |true |
    |db3|tab3 |     |
    
    The application will check the contents of a table that follows the above structure to correctly tag S3 objects(tables).

2. `--data-bucket` This is the bucket name where the dataset resides that requires tagging

3. `--data-s3-prefix` This is the S3 prefix(can be partial) that the application will crawl through to find objects(tables) to tag with the values from the CSV.  
   
    Example: `Data_product_output/2021-01-28/`. The application will crawl through any objects it finds in that prefix.  

    Partial example: `Data_product_output/2021-01-28/database_one/examp` The application will crawl through all prefixes that start with `examp`. It will find all objects in `example_one` and `example_two` if they exist

4. `--log-level` Optional argument. Default is `INFO`

5. `--environment` The environment the app is being ran in. e.g. Development

6. `--application` The name to give to the application. This will show up in the logs

## Environment variables

The required environment variables. They are replaced with the parameters passed in from the above arguments

|Variable name|Example|Description|
|---|:---:|---:|
|csv_location| s3://bucket/example/csv_file.csv |The full path to the CSV |
|data_bucket| NOT_SET |Bucket name |
|data_s3_prefix| NOT_SET |Prefix to crawl |
|log_level| INFO |The desired log level, INFO or DEBUG |
|environment| NOT_SET | The environment the app runs in. e.g. Development |
|application| NOT_SET |The name of the application | 

## Assumptions 

There are some assumptions made about the structure of the S3 objects and the data in the CSV  

1. S3 structure  

    The structure of objects in the prefix are expected to look like this: `folder_name/db_name.db/table_name/<objects-to-tag>`  
    `<objects-to-tag>` are one or more part files of `table_name` that make up the table. 
    
2. CSV database names

    The output from the data products creates databases with a `.db` suffix. Therefore, the database names in the csv are expected to follow the same syntax.  
    eg. `database_name.db`
    

The application is deployed to [DockerHub](https://hub.docker.com/repository/docker/dwpdigital/dataworks-s3-object-tagger), after which it is mirrored to AWS ECR.

After cloning this repo, please run:  
`make bootstrap`
