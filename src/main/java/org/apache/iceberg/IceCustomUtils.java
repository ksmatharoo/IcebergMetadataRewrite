package org.apache.iceberg;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.io.ContentCache;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;

import static org.apache.iceberg.ManifestFiles.cachingEnabled;
import static org.apache.iceberg.ManifestFiles.contentCache;

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
                writer.existing(dataFile, entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
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


    public static ManifestReader<DeleteFile> readDeleteManifestReader(ManifestFile manifest, String path,
                                                                      FileIO io,
                                                                      Map<Integer, PartitionSpec> specsById) {

        //log. info("ManifestContent. DELETES ()", manifest);
        Preconditions.checkArgument(manifest.content() == ManifestContent.DELETES,
                "Cannot read a delete manifest with a ManifestReader: %s", manifest);
        InputFile file = newInputFile(io, path, manifest.length());
        InheritableMetadata inheritableMetadata = InheritableMetadataFactory.fromManifest(manifest);
        ManifestReader manifestReader = new ManifestReader(file, manifest.partitionSpecId(),
                specsById, inheritableMetadata, ManifestReader.FileType.DELETE_FILES);
        return manifestReader;
    }


    public static void updateDeleteManifest(ManifestReader<DeleteFile> manifestReader,
                                            ManifestWriter<DeleteFile> writer,
                                            PartitionSpec spec, String sourcePrefix, String targetPrefix) {
        manifestReader.entries().forEach(
                entry -> appendDeleteEntry(entry, writer, spec, sourcePrefix, targetPrefix));
    }

    public static void appendDeleteEntry(ManifestEntry<DeleteFile> entry, ManifestWriter<DeleteFile> writer,
                                         PartitionSpec spec, String sourcePrefix, String targetPrefix) {
        DeleteFile deleteFile = entry.file();
        DeleteFile updateDeleteFile = null;
        String dataFilePath = deleteFile.path().toString();
        if (dataFilePath.startsWith(sourcePrefix)) {
            dataFilePath = TableMetadataUtil.newPath(dataFilePath, sourcePrefix, targetPrefix);
            List<Integer> equalityFieldIds = deleteFile.equalityFieldIds();
            int[] deleteFileEqualityFieldIds = null;
            if (equalityFieldIds != null) {
                deleteFileEqualityFieldIds = new int[equalityFieldIds.size()];
                for (int i = 0; i < equalityFieldIds.size(); i++) {
                    deleteFileEqualityFieldIds[i] = equalityFieldIds.get(i);
                }
            }
            Metrics metrics = new Metrics(deleteFile.recordCount(), deleteFile.columnSizes(), deleteFile.valueCounts(),
                    deleteFile.nullValueCounts(), deleteFile.nanValueCounts());

            updateDeleteFile = new GenericDeleteFile(deleteFile.specId(),
                    deleteFile.content(), dataFilePath, deleteFile.format(),
                    (PartitionData) deleteFile.partition(), deleteFile.fileSizeInBytes(),
                    metrics, deleteFileEqualityFieldIds,
                    deleteFile.sortOrderId(),
                    deleteFile.keyMetadata());
            DeleteFile updateDeleteFile1 = updateDeleteFile == null ? deleteFile : updateDeleteFile;
            log.info("deleteFile :(}", deleteFile);
            log.info("updateDeleteFile1 : (}", updateDeleteFile1);
            switch (entry.status()) {
                case ADDED:
                    writer.add(updateDeleteFile1);
                    break;
                case EXISTING:
                    writer.existing(updateDeleteFile1, entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
                    break;
                case DELETED:
                    writer.delete(updateDeleteFile1, entry.dataSequenceNumber(), entry.fileSequenceNumber());
                    break;
            }
        }
    }

}

