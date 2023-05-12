package org.ksm.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.*;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.LocationUtil;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

@Slf4j
public class MetadataUpdater {
    public static String DEFAULT_METADATA_DIR_NAME = "metadata";
    public static String DEFAULT_TARGET_METADATA_DIR_NAME = "updated";

    String sourceBasePath;

    Configuration conf;

    /**
     * if this is not given can be read from metadata.json file
     **/
    String sourcePrefixToReplace;

    /**
     * if targetBasePath not given then targetBasePath = sourceBasePath
     * and metadata_<env> folder created within and updated files reside there
     */
    String targetPrefixPath;

    /**
     * metadata folder will be created with as suffix metadata_<env>
     * if not given then default would be metadata_updated
     */
    String targetEnvName;

    /**
     * if source and target are different dir then setting this flag can write data in metadata dir
     * **/
    boolean writeToOrigMetadataDir;

    FileIO fileIO;

    int formatVersion;

    Map<String, ManifestListHelper> manifestListVsSnapHot = new HashMap<>();
    Map<Integer, PartitionSpec> partitionSpecMap = new HashMap<>();
    Map<String, ManifestFile> manifestFilesToRewrite = new HashMap<>();

    private MetadataUpdater(MetadataUpdaterBuilder builder) {
        this.sourceBasePath = builder.sourceBasePath;
        this.conf = builder.conf;
        fileIO = getFileIO(builder.conf, builder.properties);

        if (Objects.isNull(builder.targetEnvName)) {
            targetEnvName = DEFAULT_TARGET_METADATA_DIR_NAME;
        } else {
            targetEnvName = builder.targetEnvName;
        }
        if (Objects.isNull(builder.targetBasePath)) {
            this.targetPrefixPath = builder.sourceBasePath;
        } else {
            this.targetPrefixPath = builder.targetBasePath;
        }
        if (builder.writeToOrigMetadataDir) {
            if(sourceBasePath.equalsIgnoreCase(targetPrefixPath)){
                this.writeToOrigMetadataDir = false;
                log.warn("sourceBasePath :{} and  targetPrefixPath : {} path are same metadata can't overwrite original " +
                        "files ",sourceBasePath, targetPrefixPath);
            } else {
                this.writeToOrigMetadataDir = true;
            }
        }
    }

    public static class MetadataUpdaterBuilder {

        String sourceBasePath;
        Configuration conf;
        Map<String, String> properties;

        String targetEnvName;
        String targetBasePath;

        boolean writeToOrigMetadataDir;

        public MetadataUpdaterBuilder withTargetEnvName(String targetEnvName) {
            this.targetEnvName = targetEnvName;
            return this;
        }

        public MetadataUpdaterBuilder withTargetBasePath(String targetBasePath) {
            this.targetBasePath = targetBasePath;
            return this;
        }

        public MetadataUpdaterBuilder withWriteToOrigMetadataDir(boolean writeToOrigMetadataDir) {
            this.writeToOrigMetadataDir = writeToOrigMetadataDir;
            return this;
        }

        public MetadataUpdaterBuilder(String sourceBasePath, Configuration conf, Map<String, String> properties) {
            this.sourceBasePath = sourceBasePath;
            this.conf = conf;
            this.properties = properties;
        }

        public MetadataUpdater build() {
            return new MetadataUpdater(this);
        }
    }

    public void updateBasePathInMetadata() {

        try {
            FileSystem fileSystem = FileSystem.get(new URI(sourceBasePath), conf);
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(sourceBasePath +
                    "/" + DEFAULT_METADATA_DIR_NAME), false);
            Set<String> metadataPaths = new HashSet<>();
            Set<String> manifestListPaths = new HashSet<>();
            Set<String> manifestPaths = new HashSet<>();
            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                Path path = fileStatus.getPath();
                String fileName = path.getName();
                if (fileName.endsWith(".json")) {
                    metadataPaths.add(path.toString());
                } else if (fileName.startsWith("snap") && fileName.endsWith(".avro")) {
                    manifestListPaths.add(path.toString());
                } else {
                    manifestPaths.add(path.toString());
                }
            }
            sourcePrefixToReplace = getSourcePrefixToReplace(metadataPaths.toArray(new String[0]));
            log.info("sourcePrefixToReplace: {} will be replaced with: {}", sourcePrefixToReplace, targetPrefixPath);
            updateMetadataJson(metadataPaths.toArray(new String[0]));
            updateManifestList(manifestListPaths.toArray(new String[0]));
            updateManifestFiles(manifestPaths.toArray(new String[0]));

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSourcePrefixToReplace(String[] jsonPaths) {
        formatVersion = -1;
        String previousFileLocation = null;
        //check few json files for location
        for (String jsonPath : jsonPaths) {
            TableMetadata tableMetadata = TableMetadataParser.read(fileIO, jsonPath);
            String location = tableMetadata.location();
            if (formatVersion == -1) {
                formatVersion = tableMetadata.formatVersion();
            } else if (formatVersion != tableMetadata.formatVersion()) {
                String msg = String.format("Issue in metadata file [%s], metadata formatVersion should be same in all metadata files",
                        jsonPath);
                log.error(msg);
                throw new RuntimeException(msg);
            }
            if (previousFileLocation == null) {
                previousFileLocation = location;
            } else if (location.equalsIgnoreCase(previousFileLocation)) {
                return location;
            } else {
                String msg = String.format("Issue in metadata file [%s], metadata location should be same in all metadata files",
                        jsonPath);
                log.error(msg);
                throw new RuntimeException(msg);
            }
        }
        return previousFileLocation;
    }

    private String getOutputDir() {

        String path  = writeToOrigMetadataDir ?
            String.format("%s/metadata/",targetPrefixPath) :
                String.format("%s/metadata_%s/", targetPrefixPath, targetEnvName);
        return path;
    }

    private String getFinalMetadataTargetPath() {

        String path  = writeToOrigMetadataDir ?
                String.format("%s/metadata/",targetPrefixPath) :
                String.format("%s/metadata_%s/", targetPrefixPath, targetEnvName);
        return path;

    }

    private String getFinalMetadataSourcePath() {

        String path = String.format("%s/metadata/",targetPrefixPath) ;
        return path;

    }
    private void updateMetadataJson(String[] jsonPaths) {

        for (String jsonPath : jsonPaths) {
            String fileName = fileName(jsonPath);
            TableMetadata tableMetadata = TableMetadataParser.read(fileIO, jsonPath);
            TableMetadata updated = TableMetadataUtil.replacePaths(tableMetadata, getFinalMetadataSourcePath(),
                    getFinalMetadataTargetPath(), fileIO);

            TableMetadataParser.overwrite(updated, fileIO.newOutputFile(getOutputDir() + fileName));

            List<Snapshot> snapshots = tableMetadata.snapshots();

            partitionSpecMap.putAll(tableMetadata.specsById());
            /**
             * store manifestListName file name and snapshot info in map to use in manifestList writing
             * */
            for (Snapshot snapshot : snapshots) {
                String manifestListName = fileName(snapshot.manifestListLocation());
                manifestListVsSnapHot.put(manifestListName,
                        new ManifestListHelper(tableMetadata.formatVersion(), snapshot));
            }
        }
    }

    private void updateManifestList(String[] manifestListPaths) {

        for (String manifestListLocation : manifestListPaths) {
            String manifestListFileName = fileName(manifestListLocation);
            List<ManifestFile> manifestFiles = IceCustomUtils.readManifestLists(fileIO.newInputFile(manifestListLocation));


            ManifestListHelper manifestListHelper = manifestListVsSnapHot.get(manifestListFileName);
            int formatVersion = manifestListHelper.formatVersion;
            Snapshot snapshot = manifestListHelper.snapshot;

            String path = snapshot.manifestListLocation();

            OutputFile outputFile = fileIO.newOutputFile(getOutputDir() + manifestListFileName);


            try (FileAppender<ManifestFile> writer = IceCustomUtils.writeManifestLists(formatVersion, outputFile,
                    snapshot.snapshotId(), snapshot.parentId(), snapshot.sequenceNumber())) {
                for (ManifestFile manifestFileObj : manifestFiles) {
                    // need to get the ManifestFile object for manifest manifestFileObj rewriting
                    //if (manifestFilePaths.contains(manifestFileObj.path()))
                    {
                        String fileName = fileName(manifestFileObj.path());
                        //manifestVsPartitionSpec.put(fileName,manifestFileObj.partitionSpecId());
                        manifestFilesToRewrite.put(fileName, manifestFileObj);
                    }
                    ManifestFile newFile = manifestFileObj.copy();
                    if (newFile.path().startsWith(sourcePrefixToReplace)) {
                        ((StructLike) newFile).set(0, newPath(newFile.path(),
                                getFinalMetadataSourcePath(), getFinalMetadataTargetPath()));
                    }
                    writer.add(newFile);
                }

            } catch (IOException e) {
                throw new UncheckedIOException("Failed to rewrite the manifest list file " + path, e);
            }
        }
    }

    private void updateManifestFiles(String[] manifestFiles) throws IOException {
        for (String manifestFilePath : manifestFiles) {
            String manifestFileName = fileName(manifestFilePath);

            ManifestFile manifestFile = manifestFilesToRewrite.get(manifestFileName);
            ManifestReader<DataFile> manifestReader = IceCustomUtils.readManifestReader(manifestFile,
                    manifestFilePath, fileIO, null);

            OutputFile outputFile = fileIO.newOutputFile(getOutputDir() + manifestFileName);

            Long snapshotId = manifestFile.snapshotId();


            //todo store format version some where
            //int formatVersion = formatVersion;
            int partitionSpecId = manifestFile.partitionSpecId();
            PartitionSpec partitionSpec = partitionSpecMap.get(partitionSpecId);

            ManifestWriter<DataFile> manifestWriter =
                    ManifestFiles.write(formatVersion, partitionSpec, outputFile, snapshotId);
            try {
                IceCustomUtils.updateManifest(manifestReader, manifestWriter,
                        partitionSpec, sourcePrefixToReplace, targetPrefixPath);
            } finally {
                manifestWriter.close();
            }
            ManifestFile updatedManifest = manifestWriter.toManifestFile();
        }
    }

    String fileName(String path) {
        String filename = path;
        int lastIndex = path.lastIndexOf(File.separator);
        if (lastIndex != -1) {
            filename = path.substring(lastIndex + 1);
        }
        return filename;
    }

    String newPath(String path, String sourcePrefix, String targetPrefix) {
        return path.replaceFirst(sourcePrefix, targetPrefix);
    }

    private FileIO getFileIO(Configuration conf, Map<String, String> properties) {

        if (properties.containsKey("uri")) {
            conf.set(HiveConf.ConfVars.METASTOREURIS.varname, (String) properties.get("uri"));
        }

        if (properties.containsKey("warehouse")) {
            conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
                    LocationUtil.stripTrailingSlash((String) properties.get("warehouse")));
        }

        String fileIOImpl = (String) properties.get("io-impl");
        FileIO fileIO = (FileIO) (fileIOImpl == null ? new HadoopFileIO(conf) :
                CatalogUtil.loadFileIO(fileIOImpl, properties, conf));
        return fileIO;
    }
}