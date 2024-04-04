package org.apache.iceberg;

import lombok.Data;

import java.io.Serializable;

@Data
public class SnapshotInfo implements Serializable {

    int formatVersion;
    String manifestListLocation;
    long snapshotId;
    Long parentId;
    long sequenceNumber;

    public SnapshotInfo(int formatVersion, String manifestListLocation, long snapshotId, Long parentId,
                        long sequenceNumber) {
        this.formatVersion = formatVersion;
        this.manifestListLocation = manifestListLocation;
        this.snapshotId = snapshotId;
        this.parentId = parentId;
        this.sequenceNumber = sequenceNumber;
    }
}
