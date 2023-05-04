package org.ksm.integration;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

public class Utils {

    public static SparkSession getSparkSession()  {
        SparkSession sparkSession = SparkSession.builder()
                .appName("IcebergTest")
                .master("local[1]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                //.config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.hadoop.CustomHadoopFileIO")
                .config("spark.sql.warehouse.dir", "/home/ksingh/ksingh/IcebergTest/data")
                .config("spark.hive.metastore.uris", "thrift://172.19.0.5:9083")
                .enableHiveSupport()
                .getOrCreate();

        return sparkSession;
    }

}
