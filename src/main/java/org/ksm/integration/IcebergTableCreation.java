package org.ksm.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class IcebergTableCreation {

    HiveCatalog hiveCatalog;

    public IcebergTableCreation(HiveCatalog hiveCatalog) {
        this.hiveCatalog = hiveCatalog;
    }

    public Table createTable(Schema schema, List<String> partitionColumns, TableIdentifier identifier,
                             String baseLocation) {

        if (!hiveCatalog.tableExists(identifier)) {

            String location = baseLocation + "/" + identifier.name();

            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.put("format-version", "2");

            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
            partitionColumns.forEach(field -> builder.identity(field));
            PartitionSpec spec = builder.build();

            Table table = hiveCatalog.createTable(identifier, schema, spec, location, tableProperties);
            log.info("table :{} created at : {}", identifier, table.location());
            return table;
        } else {
            Table table = hiveCatalog.loadTable(identifier);
            log.info("table :{} already exists, at : {}", identifier, table.location());
            return table;
        }
    }

    public Table createTableFromDataset(Dataset<Row> input, List<String> partitionColumns, TableIdentifier identifier,
                                        String baseLocation) {
        if (!hiveCatalog.tableExists(identifier)) {

            Schema schema = SparkSchemaUtil.convert(input.schema());

            String location = baseLocation + "/" + identifier.name();

            Map<String, String> tableProperties = new HashMap<String, String>();
            tableProperties.put("format-version", "2");

            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

            partitionColumns.forEach(field -> builder.identity(field));
            PartitionSpec spec = builder.build();

            Table table = hiveCatalog.createTable(identifier, schema, spec, location, tableProperties);
            log.info("table :{} created at : {}", identifier, table.location());
            return table;
        } else {
            Table table = hiveCatalog.loadTable(identifier);
            log.info("table :{} already exists, at : {}", identifier, table.location());
            return table;
        }
    }
}
