package org.ksm.integration;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


@Slf4j
public class Utils {

    public static SparkSession getSparkSession(String wareHousePath, String thriftURL) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("IcebergTest")
                .master("local[1]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.warehouse.dir", wareHousePath)
                .config("spark.hive.metastore.uris", thriftURL)
                .enableHiveSupport()
                .getOrCreate();

        return sparkSession;
    }

    /**
     * all column names converted to lower case
     **/
    public static Dataset<Row> readCSVFileWithoutDate(SparkSession spark, String path) {

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

    public static boolean moveDirectory(SparkSession session, String srcPathStr, String destPathStr) {
        Configuration conf = session.sparkContext().hadoopConfiguration();
        Path srcPath = new Path(srcPathStr);
        Path destPath = new Path(destPathStr);

        try {
            FileSystem fs = FileSystem.get(conf);
            boolean success = fs.rename(srcPath, destPath);
            fs.close();
            return success;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean deleteDirectory(SparkSession session, String directoryPathStr) {
        Configuration conf = session.sparkContext().hadoopConfiguration();
        Path directoryPath = new Path(directoryPathStr);
        boolean success = false;
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(directoryPath)) {
                success = fs.delete(directoryPath, true); // 'true' to recursively delete
                log.info("deleted dir : {}", directoryPathStr);
            }
            fs.close();
            return success;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * table name should be full table name <dbName>.<tableName>
     **/
    public static void dropTable(SparkSession session, String... tableNames) {
        for (String tab : tableNames) {
            try {
                session.sql("drop table if exists " + tab).collect();
            } catch (Exception e) {
                log.warn("table drop for table name :{} got exception : ", tab, e);
            }
        }
    }

}
