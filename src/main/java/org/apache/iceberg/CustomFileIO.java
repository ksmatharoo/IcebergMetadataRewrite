package org.apache.iceberg;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.*;
import org.apache.iceberg.util.SerializableSupplier;

import java.util.Map;
import java.util.function.Function;

public class CustomFileIO implements FileIO, HadoopConfigurable, SupportsPrefixOperations {

    HadoopFileIO fileIO;
    String basePath;
    String[] oldBaseUri;

    public CustomFileIO(Configuration conf) {
        this(new SerializableConfiguration(conf)::get);
    }

    public CustomFileIO(SerializableSupplier<Configuration> conf) {
        this.basePath = conf.get().get("fs.baseuri", "");
        this.oldBaseUri = conf.get().get("fs.oldbaseuri", "").split(",");
        this.fileIO = new HadoopFileIO(conf.get());
    }

    public CustomFileIO() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        fileIO.initialize(properties);
    }

    @Override
    public Map<String, String> properties() {
        return fileIO.properties();
    }

    @Override
    public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> function) {
        this.fileIO.serializeConfWith(function);
    }

    @Override
    public void setConf(Configuration conf) {
        this.basePath = conf.get("fs.baseuri", "");
        this.oldBaseUri = conf.get("fs.oldbaseuri", "").split(",");
        this.fileIO = new HadoopFileIO(conf);
    }

    @Override
    public Configuration getConf() {
        return fileIO.conf();
    }

    @Override
    public Iterable<FileInfo> listPrefix(String s) {
        return fileIO.listPrefix(updatedPathIfRequired(s));
    }

    @Override
    public void deletePrefix(String s) {
        fileIO.deletePrefix(updatedPathIfRequired(s));
    }

    @Override
    public OutputFile newOutputFile(String s) {
        return fileIO.newOutputFile(updatedPathIfRequired(s));
    }

    @Override
    public void deleteFile(String s) {
        fileIO.deleteFile(updatedPathIfRequired(s));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        String s = updatedPathIfRequired(path);
        InputFile inputFile = fileIO.newInputFile(s, length);
        if (!s.equals(path)) {
            try {
                FieldUtils.writeField(inputFile, "location", path, true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return inputFile;
    }

    @Override
    public InputFile newInputFile(String path) {
        String s = updatedPathIfRequired(path);
        InputFile inputFile = fileIO.newInputFile(s);
        if (!s.equals(path)) {
            try {
                FieldUtils.writeField(inputFile, "location", path, true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return inputFile;
    }

    private String updatedPathIfRequired(String location) {
        if (StringUtils.isBlank(basePath) || location.startsWith(basePath)) {
            return location;
        }
        for (String s : oldBaseUri) {
            if (location.startsWith(s)) {
                return location.replace(s, basePath);
            }
        }
        throw new RuntimeException("Not matching with any base path" + location);
    }
}
