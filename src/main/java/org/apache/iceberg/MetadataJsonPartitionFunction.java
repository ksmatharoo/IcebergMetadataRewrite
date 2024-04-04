package org.apache.iceberg;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.ksm.integration.ManifestListHelper;
import org.ksm.integration.MetadataUpdater;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


@Log4j2
public class MetadataJsonPartitionFunction extends AbstractPartitionFunction
        implements MapPartitionsFunction<String, Row> {
    public MetadataJsonPartitionFunction(Broadcast<SerializableConfiguration> serializableConf,
                                         String sourcePrefixToReplace, String targetPrefixToReplace,
                                         String metadataOutputPath, boolean writeToOrigMetadataDir,
                                         String targetEnv, String useMetadataDir,
                                         HashMap<String, String> properties) {

        super(serializableConf, sourcePrefixToReplace, targetPrefixToReplace,
                metadataOutputPath, writeToOrigMetadataDir, targetEnv,
                useMetadataDir, properties);
    }

    @Override
    public Iterator<Row> call(Iterator<String> iterator) throws Exception {
        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Row next() {
                String jsonPath = iterator.next();
                SerializableConfiguration conf = serializableConf.getValue();
                FileIO fileIO = getFileIO(conf.get());

                log.info("json path :{}", jsonPath);
                String fileName = MetadataUpdater.fileName(jsonPath);

                TableMetadata tableMetadata = TableMetadataParser.read(fileIO, jsonPath);
                TableMetadata updated = TableMetadataUtil.replacePaths(tableMetadata, getFinalMetadataSourcePath(),
                        getFinalMetadataTargetPath(), fileIO);

                TableMetadataParser.overwrite(updated, fileIO.newOutputFile(getOutputDir() + fileName));

                List<Snapshot> snapshots = tableMetadata.snapshots();

                HashMap<Integer, PartitionSpec> partitionSpecMap = new HashMap<>();
                partitionSpecMap.putAll(tableMetadata.specsById());
                /**
                 * store manifestListName file name and snapshot info in map to use in manifestList writing
                 * */
                HashMap<String, SnapshotInfo> snapshotInfoHashMap = new HashMap<>();
                for (Snapshot snapshot : snapshots) {
                    String manifestListName = MetadataUpdater.fileName(snapshot.manifestListLocation());
                    snapshotInfoHashMap.put(manifestListName,
                            new SnapshotInfo(tableMetadata.formatVersion(),
                                    snapshot.manifestListLocation(),
                                    snapshot.snapshotId(),
                                    snapshot.parentId(),
                                    snapshot.sequenceNumber()));
                }
                byte[] snapshotInfoMapSer = SerializationUtils.serialize(snapshotInfoHashMap);
                byte[] partitionSpecMapSer = SerializationUtils.serialize(partitionSpecMap);
                return RowFactory.create(jsonPath, snapshotInfoMapSer, partitionSpecMapSer);
            }
        };
    }
}
