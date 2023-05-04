package org.ksm.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Application {

    public static void main(String[] args){

        SparkSession spark = Utils.getSparkSession();
        Map<String, String> properties = new HashMap<String, String>();
        String wareHousePath = "/home/ksingh/ksingh/IcebergMetadataRewrite/data";
        properties.put("warehouse", wareHousePath);
        properties.put("uri", "thrift://172.19.0.5:9083");

        String sourceDirPath = "/home/ksingh/ksingh/IcebergMetadataRewrite/data/employee2";
        String targetDirPath = "/home/ksingh/ksingh/IcebergMetadataRewrite/data/employee2001";
        Configuration conf = spark.sparkContext().hadoopConfiguration();

        MetadataUpdater updater = new MetadataUpdater.
                MetadataUpdaterBuilder(sourceDirPath, conf, properties).
                withTargetEnvName("aws1").
                build();

        updater.updateBasePathInMetadata();


        MetadataUpdater updater1 = new MetadataUpdater.
                MetadataUpdaterBuilder(sourceDirPath, conf, properties).
                withTargetBasePath(targetDirPath).
                withWriteToOrigMetadataDir(true).
                build();

        updater1.updateBasePathInMetadata();

        log.info("end");
    }
}
