package org.apache.iceberg;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.io.*;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;


import java.util.List;
import java.util.Map;

import static org.apache.iceberg.ManifestFiles.*;

@Slf4j
public class IceCustomUtils {

    public static ManifestListWriter writeManifestLists(int formatVersion, OutputFile manifestListFile, long snapshotId,
                                                        Long parentSnapshotId, long sequenceNumber) {
        return ManifestLists.write(formatVersion, manifestListFile, snapshotId,
                parentSnapshotId, sequenceNumber);
    }

    public static List<ManifestFile> readManifestLists(InputFile manifestList) {
        return ManifestLists.read(manifestList);
    }

    public static void appendEntry(ManifestEntry<DataFile> entry, ManifestWriter<DataFile> writer,
                                   PartitionSpec spec, String sourcePrefix, String targetPrefix) {

        DataFile dataFile = entry.file();
        String dataFilePath = dataFile.path().toString();
        if (dataFilePath.startsWith(sourcePrefix)) {
            dataFilePath = TableMetadataUtil.newPath(dataFilePath, sourcePrefix, targetPrefix);
            dataFile = DataFiles.builder(spec).copy(entry.file()).withPath(dataFilePath).build();
        }

        switch (entry.status()) {
            case ADDED:
                writer.add(dataFile);
                break;
            case EXISTING:
                writer.existing(dataFile, entry.snapshotId(), entry.sequenceNumber());
                break;
            case DELETED:
                writer.delete(dataFile, entry.dataSequenceNumber(), entry.fileSequenceNumber());
                break;
        }
    }

    public static ManifestReader<DataFile> readManifestReader(ManifestFile manifest, String path, FileIO io,
                                                              Map<Integer, PartitionSpec> specsById) {

        Preconditions.checkArgument(manifest.content() == ManifestContent.DATA,
                "Cannot read a delete manifest with a ManifestReader: %s", manifest);
        InputFile file = newInputFile(io, path, manifest.length());
        InheritableMetadata inheritableMetadata = InheritableMetadataFactory.fromManifest(manifest);
        ManifestReader manifestReader = new ManifestReader(file, manifest.partitionSpecId(), specsById, inheritableMetadata, ManifestReader.FileType.DATA_FILES);

        return manifestReader;
    }

    public static void updateManifest(ManifestReader<DataFile> manifestReader, ManifestWriter<DataFile> writer,
                                      PartitionSpec spec, String sourcePrefix, String targetPrefix) {

        manifestReader.entries().forEach(
                entry -> appendEntry(entry, writer, spec, sourcePrefix, targetPrefix));
    }

    public static InputFile newInputFile(FileIO io, String path, long length) {
        boolean enabled;
        try {
            enabled = cachingEnabled(io);
        } catch (UnsupportedOperationException var6) {
            enabled = false;
        }

        if (enabled) {
            ContentCache cache = contentCache(io);
            Preconditions.checkNotNull(cache, "ContentCache creation failed. Check that all manifest caching configurations has valid value.");
            //log.debug("FileIO-level cache stats: {}", CONTENT_CACHES.stats());

            return cache.tryCache(io, path, length);
        } else {
            return io.newInputFile(path, length);
        }
    }

}

