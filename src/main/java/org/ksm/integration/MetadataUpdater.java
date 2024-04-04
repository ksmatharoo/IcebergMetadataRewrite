package org.ksm.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.Pair;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassManifestFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

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
     **/
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
            if (sourceBasePath.equalsIgnoreCase(targetPrefixPath)) {
                this.writeToOrigMetadataDir = false;
                log.warn("sourceBasePath :{} and  targetPrefixPath : {} path are same metadata can't overwrite original " +
                        "files ", sourceBasePath, targetPrefixPath);
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

    public void updateBasePathInMetadata(SparkSession session, HiveCatalog hiveCatalog, TableIdentifier tableIdentifier, boolean useS3Listing)
            throws Exception {
        try {
            Set<String> metadataPaths = new HashSet<>();
            Set<String> manifestListPaths = new HashSet<>();
            Set<String> manifestPaths = new HashSet<>();

            Pair<List<String>, List<String>> metadataFileNames;
            if (useS3Listing) {
                metadataFileNames = getMetadataFileNamesUsingS3Listing(manifestPaths);
            } else {
                metadataFileNames = getMetadataFileNames(hiveCatalog,
                        tableIdentifier);
            }
            metadataPaths.addAll(metadataFileNames.first());
            manifestListPaths.addAll(metadataFileNames.second());

            sourcePrefixToReplace = getSourcePrefixToReplace(metadataPaths.toArray(new String[0]));
            log.info("sourcePrefixToReplace: {} will be replaced with: {}", sourcePrefixToReplace, targetPrefixPath);
            updateMetadataJson(metadataPaths.toArray(new String[0]));
            updateManifestList(manifestListPaths.toArray(new String[0]));
            updateManifestFiles(manifestPaths.toArray(new String[0]));

        } catch (Exception e) {
            log.error("exception caught : ", e);
            throw e;
        }
    }

    public void updateBasePathInMetadataSpark(SparkSession session, HiveCatalog hiveCatalog,
                                              TableIdentifier tableIdentifier, boolean useS3Listing) throws Exception {
        try {
            useS3Listing = false;
            String userMetadataDir = "metadata";
            // todo
            String metadataOutputPath = this.sourceBasePath;


            Set<String> metadataPaths = new HashSet<>();
            Set<String> manifestListPaths = new HashSet<>();
            Set<String> manifestPaths = new HashSet<>();

            Pair<List<String>, List<String>> metadataFileNames;
            if (useS3Listing) {
                metadataFileNames = getMetadataFileNamesUsingS3Listing(manifestPaths);
            } else {
                metadataFileNames = getMetadataFileNames(hiveCatalog,
                        tableIdentifier);
            }
            metadataPaths.addAll(metadataFileNames.first());
            manifestListPaths.addAll(metadataFileNames.second());

            sourcePrefixToReplace = getSourcePrefixToReplace(metadataPaths.toArray(new String[0]));
            log.info("sourcePrefixToReplace: {} will be replaced with: {}", sourcePrefixToReplace, targetPrefixPath);
            /*updateMetadataJson(metadataPaths.toArray(new String[0]));
            updateManifestList(manifestListPaths.toArray(new String[0]));
            updateManifestFiles(manifestPaths.toArray(new String[0]));*/

            List<String> metadataJsonList = metadataFileNames.first();
            List<String> manifestList = metadataFileNames.second();
            Dataset<String> datasetOfMetadataJson = session.createDataset(metadataJsonList, Encoders.STRING());
            StructType outSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("path", DataTypes.StringType, false),
                    DataTypes.createStructField("snapshots", DataTypes.BinaryType, false),
                    DataTypes.createStructField("partSpec", DataTypes.BinaryType, false)
            });
            Configuration conf = session.sparkContext().hadoopConfiguration();
            Broadcast<SerializableConfiguration> serConf = session.sparkContext().broadcast(new SerializableConfiguration(conf),
                    ClassManifestFactory.fromClass(SerializableConfiguration.class));

            HashMap<String, String> properties = new HashMap<>();
            properties.putAll(fileIO.properties());

            Dataset<Row> rowDataset = datasetOfMetadataJson.repartition(metadataJsonList.size()).
                    mapPartitions(
                            new MetadataJsonPartitionFunction(serConf,
                                    this.sourcePrefixToReplace,
                                    this.targetPrefixPath,
                                    metadataOutputPath,
                                    this.writeToOrigMetadataDir,
                                    this.targetEnvName,
                                    userMetadataDir,
                                    properties),
                            RowEncoder.apply(outSchema));
            List<Row> collectedRows = rowDataset.collectAsList();

            HashMap<String, SnapshotInfo> snapshotInfo = new HashMap<>();
            HashMap<Integer, PartitionSpec> integerPartitionSpec = new HashMap<>();
            for (Row row : collectedRows) {
                byte[] out1 = (byte[]) row.get(1);
                byte[] out2 = (byte[]) row.get(2);
                snapshotInfo.putAll(
                        (HashMap<String, SnapshotInfo>) SerializationUtils.deserialize(out1));
                integerPartitionSpec.putAll(
                        (HashMap<Integer, PartitionSpec>) SerializationUtils.deserialize(out2));
            }

            int formatVersion = getFormatVersion(snapshotInfo);
            Dataset<String> manifestListPathDf = session.createDataset(manifestList, Encoders.STRING());
            StructType outSchemaPaths = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("path", DataTypes.StringType, false),
                    DataTypes.createStructField("manifest", DataTypes.BinaryType, false)
            });
            Dataset<Row> updatedManifestListDf = manifestListPathDf.repartition(manifestList.size()).
                    mapPartitions(new ManifestListPartitionFunction(serConf,
                                    this.sourcePrefixToReplace,
                                    this.targetPrefixPath,
                                    metadataOutputPath,
                                    this.writeToOrigMetadataDir,
                                    this.targetEnvName,
                                    userMetadataDir,
                                    properties,
                                    snapshotInfo,
                                    formatVersion)
                            , RowEncoder.apply(outSchemaPaths));

            List<Row> updatedManifestList = updatedManifestListDf.collectAsList();

            HashMap<String, ManifestFile> manifestFileMap = new HashMap();
            for (Row row : updatedManifestList) {
                byte[] out1 = (byte[]) row.get(1);
                HashMap<String, ManifestFile> deserialize = (HashMap<String, ManifestFile>) SerializationUtils.deserialize(out1);
                manifestFileMap.putAll(deserialize);
            }
            List<ManifestFile> manifestFileList = manifestFileMap.values().stream().collect(Collectors.toList());
            StructType manifestFileOutSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("manifest", DataTypes.StringType, false),
            });
            Encoder<ManifestFile> manifestEncoder = Encoders.javaSerialization(ManifestFile.class);
            Dataset<ManifestFile> manifestFileDataset = session.createDataset(manifestFileList, manifestEncoder);

            Dataset<Row> manifestWriteOutPut = manifestFileDataset.repartition(manifestFileList.size())
                    .mapPartitions(new ManifestPartitionFunction(serConf,
                                    this.sourcePrefixToReplace,
                                    this.targetPrefixPath,
                                    metadataOutputPath,
                                    this.writeToOrigMetadataDir,
                                    this.targetEnvName,
                                    userMetadataDir,
                                    properties,
                                    formatVersion, integerPartitionSpec),
                            RowEncoder.apply(manifestFileOutSchema));
            List<Row> collected = manifestWriteOutPut.collectAsList();
            System.out.println("finished");

        } catch (Exception e) {
            log.error("exception caught : ", e);
            throw e;
        }
    }


    private int getFormatVersion(HashMap<String, SnapshotInfo> snapshotInfo) {

        List<Integer> collect = snapshotInfo.values().stream().map(k -> k.getFormatVersion()).
                distinct().collect(Collectors.toList());
        if (collect.size() > 1) {
            String join = org.apache.commons.lang3.StringUtils.join(collect.toArray(), "");
            throw new RuntimeException(String.format("Multiple FormatVersion found : %s ", join));
        }
        log.info("detected format version : (", collect.get(0));
        return collect.get(0);
    }

    public Pair<List<String>, List<String>> getMetadataFileNamesUsingS3Listing(Set<String> manifestPaths) {
        Set<String> metadataPaths = new HashSet<>();
        Set<String> manifestListPaths = new HashSet<>();
        //Set<String> manifestPaths = new HashSet<>();
        try {
            FileSystem fileSystem = FileSystem.get(new URI(sourceBasePath), conf);
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(sourceBasePath +
                    "/" + DEFAULT_METADATA_DIR_NAME), false);
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
        } catch (Exception e) {

        }
        return Pair.of(Arrays.asList(metadataPaths.toArray(new String[0])),
                Arrays.asList(manifestListPaths.toArray(new String[0])));
    }


    public Pair<List<String>, List<String>> getMetadataFileNames(HiveCatalog hiveCatalog, TableIdentifier
            tableIdentifier) {

        TableOperations tableOperations = hiveCatalog.newTableOps(tableIdentifier);
        TableMetadata tableMetadata = tableOperations.refresh();

        //get metadata.json files
        List metadataJsonList = new ArrayList<String>();
        //active snapshot at index 0
        metadataJsonList.add(tableMetadata.metadataFileLocation());

        List<TableMetadata.MetadataLogEntry> metadataLogEntries = tableMetadata.previousFiles();
        metadataLogEntries.forEach(e -> {
            metadataJsonList.add(e.file());
        });

        //get snapshots list
        List<String> snapshotsList = new ArrayList<>();
        List<Snapshot> snapshots = tableMetadata.snapshots();
        snapshots.forEach(e -> {
            snapshotsList.add(e.manifestListLocation());
        });
        return Pair.of(metadataJsonList, snapshotsList);
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

        String path = writeToOrigMetadataDir ?
                String.format("%s/metadata/", targetPrefixPath) :
                String.format("%s/metadata_%s/", targetPrefixPath, targetEnvName);
        return path;
    }

    private String getFinalMetadataTargetPath() {

        String path = writeToOrigMetadataDir ?
                String.format("%s/metadata/", targetPrefixPath) :
                String.format("%s/metadata_%s/", targetPrefixPath, targetEnvName);
        return path;

    }

    private String getFinalMetadataSourcePath() {

        String path = String.format("%s/metadata/", targetPrefixPath);
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

    public static String fileName(String path) {
        String filename = path;
        int lastIndex = path.lastIndexOf(File.separator);
        if (lastIndex != -1) {
            filename = path.substring(lastIndex + 1);
        }
        return filename;
    }

    public static String newPath(String path, String sourcePrefix, String targetPrefix) {
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