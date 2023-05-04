package org.apache.iceberg;

import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

@Slf4j
public class TableMetadataUtil {
    private TableMetadataUtil() {
    }

    public static TableMetadata replacePaths(TableMetadata metadata,
                                             String sourcePrefix,
                                             String targetPrefix,
                                             FileIO io) {
        String newLocation = newPath(metadata.location(), sourcePrefix, targetPrefix);
        List<Snapshot> newSnapshots = updatePathInSnapshots(metadata, sourcePrefix, targetPrefix, io);
        List<MetadataLogEntry> metadataLogEntries = updatePathInMetadataLogs(metadata, sourcePrefix, targetPrefix);
        long snapshotId = metadata.currentSnapshot() == null ? -1 : metadata.currentSnapshot().snapshotId();
        Map<String, String> properties = updateProperties(metadata.properties(), sourcePrefix, targetPrefix);

        return new TableMetadata(null, metadata.formatVersion(), metadata.uuid(),
                newLocation, metadata.lastSequenceNumber(), metadata.lastUpdatedMillis(), metadata.lastColumnId(),
                metadata.currentSchemaId(), metadata.schemas(), metadata.defaultSpecId(), metadata.specs(),
                metadata.lastAssignedPartitionId(), metadata.defaultSortOrderId(), metadata.sortOrders(),
                properties, snapshotId, newSnapshots, metadata.snapshotLog(), metadataLogEntries, metadata.refs(),
                metadata.statisticsFiles(),metadata.changes());
    }

    private static Map<String, String> updateProperties(Map<String, String> tableProperties,
                                                        String sourcePrefix,
                                                        String targetPrefix) {
        Map properties = Maps.newHashMap(tableProperties);
        updatePathInProperty(properties, sourcePrefix, targetPrefix, TableProperties.OBJECT_STORE_PATH);
        updatePathInProperty(properties, sourcePrefix, targetPrefix, TableProperties.WRITE_FOLDER_STORAGE_LOCATION);
        updatePathInProperty(properties, sourcePrefix, targetPrefix, TableProperties.WRITE_DATA_LOCATION);
        updatePathInProperty(properties, sourcePrefix, targetPrefix, TableProperties.WRITE_METADATA_LOCATION);

        return properties;
    }

    private static void updatePathInProperty(Map<String, String> properties, String sourcePrefix, String targetPrefix,
                                             String propertyName) {
        if (properties.containsKey(propertyName)) {
            properties.put(propertyName, newPath(properties.get(propertyName), sourcePrefix, targetPrefix));
        }
    }

    private static List<MetadataLogEntry> updatePathInMetadataLogs(TableMetadata metadata,
                                                                   String sourcePrefix,
                                                                   String targetPrefix) {
        List<MetadataLogEntry> metadataLogEntries = Lists.newArrayListWithCapacity(metadata.previousFiles().size());
        for (MetadataLogEntry metadataLog : metadata.previousFiles()) {
            MetadataLogEntry newMetadataLog = new MetadataLogEntry(metadataLog.timestampMillis(), newPath(metadataLog.file(),
                    sourcePrefix, targetPrefix));
            metadataLogEntries.add(newMetadataLog);
        }
        return metadataLogEntries;
    }

    private static List<Snapshot> updatePathInSnapshots(TableMetadata metadata,
                                                        String sourcePrefix,
                                                        String targetPrefix,
                                                        FileIO io) {
        List<Snapshot> newSnapshots = Lists.newArrayListWithCapacity(metadata.snapshots().size());
        for (Snapshot snapshot : metadata.snapshots()) {
            String newManifestListLocation = newPath(snapshot.manifestListLocation(), sourcePrefix, targetPrefix);
            Snapshot newSnapshot = new BaseSnapshot(snapshot.sequenceNumber(), snapshot.snapshotId(),
                    snapshot.parentId(), snapshot.timestampMillis(), snapshot.operation(), snapshot.summary(),
                    snapshot.schemaId(), newManifestListLocation);
            newSnapshots.add(newSnapshot);
        }
        return newSnapshots;
    }

     static String newPath(String path, String sourcePrefix, String targetPrefix) {
        return path.replaceFirst(sourcePrefix, targetPrefix);
    }
}