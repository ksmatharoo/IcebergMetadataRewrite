package org.apache.iceberg;

import lombok.SneakyThrows;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.ksm.integration.MetadataUpdater;

import java.util.HashMap;
import java.util.Iterator;

public class ManifestPartitionFunction extends AbstractPartitionFunction
        implements MapPartitionsFunction<ManifestFile, Row> {


    int formatVersion;
    HashMap<Integer, PartitionSpec> integerPartitionSpec;

    public ManifestPartitionFunction(Broadcast<SerializableConfiguration> serializableConf, String sourcePrefixToReplace,
                                     String targetPrefixToReplace, String metadataOutputPath,
                                     boolean writeToOrigMetadataDir, String targetEnv,
                                     String useMetadataDir, HashMap<String, String> properties,
                                     int formatVersion,
                                     HashMap<Integer, PartitionSpec> integerPartitionSpec) {

        super(serializableConf, sourcePrefixToReplace, targetPrefixToReplace, metadataOutputPath,
                writeToOrigMetadataDir, targetEnv, useMetadataDir, properties);
        this.formatVersion = formatVersion;
        this.integerPartitionSpec = integerPartitionSpec;
    }

    @Override
    public Iterator<Row> call(Iterator<ManifestFile> iterator) throws Exception {
        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @SneakyThrows
            @Override
            public Row next() {

                FileIO fileIO = getFileIO(serializableConf.getValue().get());
                ManifestFile manifest = iterator.next();
                String manifestFilePath = manifest.path();
                String manifestFileName = MetadataUpdater.fileName(manifestFilePath);
                ManifestFile manifestFile = manifest;

                if (manifestFile.content() == ManifestContent.DATA) {
                    ManifestReader<DataFile> manifestReader = IceCustomUtils.readManifestReader(
                            manifestFile,
                            manifestFilePath, fileIO, null);
                    OutputFile outputFile = fileIO.newOutputFile(getOutputDir() + manifestFileName);
                    Long snapshotId = manifestFile.snapshotId();
                    int partitionSpecId = manifestFile.partitionSpecId();
                    PartitionSpec partitionSpec = integerPartitionSpec.get(partitionSpecId);
                    ManifestWriter<DataFile> manifestWriter =
                            ManifestFiles.write(formatVersion, partitionSpec, outputFile, snapshotId);
                    try {
                        IceCustomUtils.updateManifest(manifestReader, manifestWriter,
                                partitionSpec, sourcePrefixToReplace, targetPrefixPath);
                    } finally {
                        manifestWriter.close();
                        manifestReader.close();
                    }
                } else if (manifestFile.content() == ManifestContent.DELETES) {
                    ManifestReader<DeleteFile> manifestReader = IceCustomUtils.readDeleteManifestReader(
                            manifestFile,
                            manifestFilePath, fileIO, null);

                    OutputFile outputFile = fileIO.newOutputFile(getOutputDir() + manifestFileName);
                    Long snapshotId = manifestFile.snapshotId();
                    int partitionSpecId = manifestFile.partitionSpecId();
                    PartitionSpec partitionSpec = integerPartitionSpec.get(partitionSpecId);
                    ManifestWriter<DeleteFile> manifestWriter =
                            ManifestFiles.writeDeleteManifest(formatVersion, partitionSpec, outputFile, snapshotId);
                    try {
                        IceCustomUtils.updateDeleteManifest(manifestReader, manifestWriter, partitionSpec, sourcePrefixToReplace, targetPrefixPath);
                    } finally {
                        manifestWriter.close();
                        manifestReader.close();
                    }
                }
                return RowFactory.create("na");
            }
        };
    }
}



