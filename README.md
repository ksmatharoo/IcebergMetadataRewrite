# IcebergMetadataRewrite

this project require hive running on local machine,you can start hive with docker image 
in hiveDocker directory.

Project will rewrite iceberg metadata, it will accept the table base and update it to all metadata file.  

# Problem 
Suppose you copied an iceberg data and metadata directory from another cluster or move the table 
to another location by copying the directories, this will result in un-readable table.

# Solution
There can be following solutions
1. use custom FileIO which will change base path of all File IO operation, this is easy but found one issue in 
   iceberg, which require one small change in iceberg code repo.
2. We can rewrite iceberg metadata and register table with updated metadata.

# Dependencies
1. Spark (version 3.2)
2. Iceberg-spark-runtime-3.2_2.12 ( version 1.1.0)

# Code walk through 
This project/lib will rewrite the table metadata, Please check Application.java for main flow of the program,
main will do following. 
1. create an iceberg table from spark data frame.
2. rewrite metadata folder to metadata_<env> folder
3. register table with latest snapshot metadata_<env>/<uuid>.json


