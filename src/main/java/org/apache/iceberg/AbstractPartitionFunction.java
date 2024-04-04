package org.apache.iceberg;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;


@Log4j2
public class AbstractPartitionFunction implements Serializable {

    Broadcast<SerializableConfiguration> serializableConf;
    String sourcePrefixToReplace;
    String targetPrefixPath;
    String metadataOutputPath;
    boolean writeToOrigMetadataDir;
    String targetEnv;
    String useMetadataDir;
    HashMap<String, String> properties;

    public AbstractPartitionFunction(Broadcast<SerializableConfiguration> serializableConf,
                                     String sourcePrefixToReplace, String targetPrefixToReplace,
                                     String metadataOutputPath, boolean writeToOrigMetadataDir,
                                     String targetEnv, String useMetadataDir, HashMap<String, String> properties) {
        this.serializableConf = serializableConf;
        this.sourcePrefixToReplace = sourcePrefixToReplace;
        this.targetPrefixPath = targetPrefixToReplace;
        this.metadataOutputPath = metadataOutputPath;
        this.writeToOrigMetadataDir = writeToOrigMetadataDir;
        this.targetEnv = targetEnv;
        this.useMetadataDir = useMetadataDir;
        this.properties = properties;
    }


    protected String getFinalMetadataSourcePath() {
        String path = String.format("%s/%s", sourcePrefixToReplace, useMetadataDir);
        return path;
    }

    protected String getOutputDir() {
        String out = Objects.nonNull(metadataOutputPath) ? metadataOutputPath : targetPrefixPath;
        log.info("out base path : {}", out);
        String path = writeToOrigMetadataDir ?
                String.format("%s/metadata/", out) :
                String.format("%s/metadata_%s/", out, targetEnv);
        return path;
    }

    protected String getFinalMetadataTargetPath() {
        String path = writeToOrigMetadataDir ?
                String.format("%s/metadata", targetPrefixPath) :
                String.format("%s/metadata_%s", targetPrefixPath, targetEnv);
        return path;
    }


    protected FileIO getFileIO(Configuration conf) {
        if (properties.containsKey("uri")) {
            conf.set(HiveConf.ConfVars.METASTOREURIS.varname, properties.get("uri"));
        }
        if (properties.containsKey("warehouse")) {
            conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                    LocationUtil.stripTrailingSlash(properties.get("warehouse")));
        }
        String fileIOImpl = properties.get("io-impl");
        FileIO fileIO = fileIOImpl == null ? new HadoopFileIO(conf) :
                CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
        return fileIO;
    }
}
