package org.ksm.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Application {

    public static void main(String[] args) throws NoSuchTableException {

        SparkSession spark = Utils.getSparkSession();

        Map<String, String> properties = new HashMap<String, String>();
        String userDirectory = System.getProperty("user.dir");
        String dir = "/data";
        String wareHousePath = userDirectory + dir;
        properties.put("warehouse", wareHousePath);
        properties.put("uri", "thrift://172.19.0.5:9083");


        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(spark.sparkContext().hadoopConfiguration());
        hiveCatalog.initialize("hive", properties);

        String dbName = "default";
        String tableName = "testTable" + System.currentTimeMillis();
        TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);

        Dataset<Row> rowDataset = Utils.readCSVFileWithoutDate(spark, "");
        IcebergTableCreation tableCreation = new IcebergTableCreation(hiveCatalog);
        tableCreation.createTableFromDataset(rowDataset,
                Arrays.asList("hire_date"),
                tableIdentifier,
                wareHousePath);


        rowDataset.writeTo(tableIdentifier.toString()).append();


        String sourceDirPath = userDirectory + dir + "/" + tableName;
        Configuration conf = spark.sparkContext().hadoopConfiguration();

        MetadataUpdater updater = new MetadataUpdater.
                MetadataUpdaterBuilder(sourceDirPath, conf, properties).
                withTargetEnvName("aws").
                build();

        updater.updateBasePathInMetadata();

        // register updated metadata.json
        //hiveCatalog.registerTable(TableIdentifier.of("default","testTable_aws1"),"<metadata path>.json");

        /*String targetDirPath = userDirectory + dir + "/testNewTable";
        MetadataUpdater updater1 = new MetadataUpdater.
                MetadataUpdaterBuilder(sourceDirPath, conf, properties).
                withTargetBasePath(targetDirPath).
                withWriteToOrigMetadataDir(true).
                build();
        updater1.updateBasePathInMetadata();*/

        log.info("end");
    }
}
