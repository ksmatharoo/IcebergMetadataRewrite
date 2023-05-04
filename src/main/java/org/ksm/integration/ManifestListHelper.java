package org.ksm.integration;

import org.apache.iceberg.Snapshot;

public class ManifestListHelper {
    int formatVersion;
    Snapshot snapshot;

    public ManifestListHelper(int formatVersion, Snapshot snapshot) {
        this.formatVersion = formatVersion;
        this.snapshot = snapshot;
    }
}