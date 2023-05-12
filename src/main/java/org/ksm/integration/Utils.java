package org.ksm.integration;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

    public static SparkSession getSparkSession()  {

        String userDirectory = System.getProperty("user.dir");
        String wareHousePath = userDirectory + "/data";
        SparkSession sparkSession = SparkSession.builder()
                .appName("IcebergTest")
                .master("local[1]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                //.config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.hadoop.CustomHadoopFileIO")
                .config("spark.sql.warehouse.dir", wareHousePath)
                .config("spark.hive.metastore.uris", "thrift://172.19.0.5:9083")
                .enableHiveSupport()
                .getOrCreate();

        return sparkSession;
    }

    /**
     * all column names converted to lower case
     * **/
    public static Dataset<Row> readCSVFileWithoutDate(SparkSession spark, String path){

        path = StringUtils.isEmpty(path) ? "src/main/resources/employee.csv" : path;

        Dataset<Row> csv = spark.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv(path);

        List<Column> columnList = Arrays.stream(csv.schema().fields()).map(
                f -> new Column(f.name().toLowerCase())
        ).collect(Collectors.toList());

        return csv.select(columnList.toArray(new Column[0])).withColumn("department_id",
                new Column("department_id").cast("String"));

    }

}
