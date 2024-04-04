package org.apache.iceberg;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.ksm.integration.MetadataUpdater;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class ManifestListPartitionFunction extends AbstractPartitionFunction
        implements MapPartitionsFunction<String, Row> {


    HashMap<String, SnapshotInfo> manifestListVsSnapshot;
    int formatVersion;

    public ManifestListPartitionFunction(Broadcast<SerializableConfiguration> serializableConf,
                                         String sourcePrefixToReplace, String targetPrefixToReplace,
                                         String metadataOutputPath, boolean writeToOrigMetadataDir,
                                         String targetEnv, String useMetadataDir, HashMap<String, String> properties,
                                         HashMap<String, SnapshotInfo> manifestListVsSnapshot,
                                         int formatVersion) {

        super(serializableConf, sourcePrefixToReplace, targetPrefixToReplace, metadataOutputPath,
                writeToOrigMetadataDir, targetEnv, useMetadataDir, properties);
        this.manifestListVsSnapshot = manifestListVsSnapshot;
        this.formatVersion = formatVersion;
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

                String row = iterator.next();
                FileIO fileIO = getFileIO(serializableConf.getValue().get());
                String manifestListLocation = row;
                String manifestListFileName = MetadataUpdater.fileName(manifestListLocation);
                List<ManifestFile> manifestFiles = IceCustomUtils.readManifestLists(
                        fileIO.newInputFile(manifestListLocation));
                SnapshotInfo snapshotInfo = manifestListVsSnapshot.get(manifestListFileName);
                int formatVersion = snapshotInfo.formatVersion;
                String path = snapshotInfo.manifestListLocation;

                OutputFile outputFile = fileIO.newOutputFile(getOutputDir() + manifestListFileName);
                HashMap<String, ManifestFile> manifestFileMap = new HashMap<>();
                try (FileAppender<ManifestFile> writer = IceCustomUtils.writeManifestLists(
                        formatVersion, outputFile,
                        snapshotInfo.snapshotId, snapshotInfo.parentId, snapshotInfo.sequenceNumber)) {
                    for (ManifestFile manifestFileObj : manifestFiles) {
                        // need to get the ManifestFile object for manifest manifestFileobj rewriting
                        String fileName = MetadataUpdater.fileName(manifestFileObj.path());
                        manifestFileMap.put(fileName, manifestFileObj);
                        ManifestFile newFile = manifestFileObj.copy();
                        if (newFile.path().startsWith(sourcePrefixToReplace)) {
                            ((StructLike) newFile).set(0, MetadataUpdater.newPath(newFile.path(),
                                    getFinalMetadataSourcePath(), getFinalMetadataTargetPath()));
                        }
                        writer.add(newFile);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to rewrite the manifest list file" + path, e);
                }
                byte[] manifestFileMapSer = SerializationUtils.serialize(manifestFileMap);
                return RowFactory.create(manifestListLocation, manifestFileMapSer);
            }
        };

    }
}
