# IcebergMetadataRewrite

This project require hive running on local machine,you can start hive with docker image 
in hiveDocker directory. Project will rewrite iceberg metadata, it will accept the table base and update it to all metadata file.  

# Problem 
Suppose you copied an iceberg table(data and metadata directory) from another cluster or move the table 
to another location by copying the directories, this will result in un-readable table.

# Solution
There can be following solutions
1. use custom FileIO which will change base path of all File IO operation (solution is in another project)
2. We can rewrite iceberg metadata and register table with updated metadata.

# Dependencies
1. Spark (version 3.2)
2. Iceberg-spark-runtime-3.2_2.12 ( version 1.1.0)

# Code walk through 
This project/lib will rewrite the table metadata, Please check IntegrationTest.java for main flow of the program.
Updated the code to rewrite files using spark in distributed way.
Updated to skip the file system listing part which can be really costly for s3 type of object store.
1. create an iceberg table from spark data frame.
2. rewrite metadata folder to metadata_<env> folder
3. register table with latest snapshot metadata_<env>/<uuid>.json