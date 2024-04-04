package org.ksm.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class IntegrationTest {

    @Test
    public void testIntegration() throws Exception {

        String userDirectory = System.getProperty("user.dir");
        String dir = "/data";
        String wareHousePath = userDirectory + dir;
        String thriftURl = "thrift://172.18.0.5:9083";
        SparkSession spark = Utils.getSparkSession(wareHousePath, thriftURl);

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", wareHousePath);
        properties.put(CatalogProperties.URI, thriftURl);
        properties.put(CatalogProperties.CATALOG_IMPL, HiveCatalog.class.getName());

        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(spark.sparkContext().hadoopConfiguration());
        hiveCatalog.initialize("hive", properties);

        String dbName = "default";
        String tableName = "test_table"; //+ System.currentTimeMillis();
        String env = "aws";
        String tableLocation = wareHousePath + "/" + tableName;
        TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);
        String newTableName = String.format("%s_%s", tableName, env);
        TableIdentifier newTableIdentifier = TableIdentifier.of(dbName, newTableName);
        String tab = "";
        try {
            tab = tableIdentifier.toString();
            spark.sql("drop table if exists " + tableIdentifier).collect();
            tab = newTableIdentifier.toString();
            spark.sql("drop table if exists " + newTableIdentifier).collect();
            Utils.deleteDirectory(spark, tableLocation);
        } catch (Exception e) {
            log.warn("table drop for table name :{} got exception : ", tab, e);
        }

        Dataset<Row> rowDataset = Utils.readCSVFileWithoutDate(spark, "");
        IcebergTableCreation tableCreation = new IcebergTableCreation(hiveCatalog);
        tableCreation.createTableFromDataset(rowDataset,
                Arrays.asList("hire_date"),
                tableIdentifier,
                wareHousePath);
        rowDataset.writeTo(tableIdentifier.toString()).append();

        String sourceDirPath = userDirectory + dir + "/" + tableName;
        Configuration conf = spark.sparkContext().hadoopConfiguration();

        Table table = hiveCatalog.loadTable(tableIdentifier);
        String metadata = ((HiveTableOperations) ((BaseTable) table).operations()).currentMetadataLocation();

        String newMetadataLocation = metadata.replace("/metadata/", String.format("/metadata_%s/", env));
        MetadataUpdater updater = new MetadataUpdater.
                MetadataUpdaterBuilder(sourceDirPath, conf, properties).
                withTargetEnvName(env).
                build();

        //updater.updateBasePathInMetadata( spark, hiveCatalog, tableIdentifier, true);
        updater.updateBasePathInMetadataSpark(spark, hiveCatalog, tableIdentifier, false);

        //register updated metadata.json
        hiveCatalog.registerTable(newTableIdentifier, newMetadataLocation);

        long newTableCount = spark.sql("select count(*) from " + newTableIdentifier).
                collectAsList().get(0).getLong(0);
        long origRowCount = spark.sql("select count(*) from " + tableIdentifier).
                collectAsList().get(0).getLong(0);

        Assert.assertTrue(newTableCount == origRowCount);
        log.info("end");
    }
}
