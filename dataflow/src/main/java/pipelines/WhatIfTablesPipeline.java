package pipelines;

// whatever inside of src-main-java is the starting point

import lombok.NonNull;

import org.apache.beam.sdk.Pipeline; 
import org.apache.beam.sdk.coders.RowCoder;

import org.apache.beam.sdk.extensions.python.PythonExternalTransform;

import org.apache.beam.sdk.transforms.Create;

import org.apache.beam.sdk.transforms.MapElements; 
import org.apache.beam.sdk.transforms.Wait;

import org.apache.beam.sdk.util.PythonCallableSource;

import org.apache.beam.sdk.values.KV;

import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.Row;

import org.apache.beam.sdk.values.TypeDescriptor; 

import org.homedepot.models.SkuData;

import org.homedepot.options. WhatIfTablesOptions;

import org.apache.beam.sdk.io.jdbc.Jdbc10;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryI0;

import java.io.*;

import java.util.List;

import java.util.stream.Collectors;

import org.homedepot.utils.secrets.MPulsePostgresSecret; 
import org.homedepot.utils.secrets.SecretManagerUtils;

import static constants.WhatIfConstants.*;

@SuppressWarnings("UnreachableCode") 
public class WhatIfTablesPipeline {

public static void of (@NonNull WhatIfTablesOptions options) {

String project = options.getProject();

String dataset= options.getDataset();

var postgresSecrets = SecretManagerUtils.getSecret(

        MPulsePostgresSecret.class, 
        options.getProject(),
        options.getPostgresSecretName(), 
        options.getPostgresSecretVersion() );

PythonCallableSource jsonToNumpy;
try (InputStream in = WhatIfTablesPipeline.class.getResourceAsStream("/transforms/python/json_to_numpy.py");

BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

        String python_code = reader.lines().collect(Collectors.joining( "\n"));
        // Use resource
    jsonToNumpy PythonCallableSource.of(python_code);

        } catch(IOException e) {
    throw new RuntimeException(e);
}

for (String table: TABLES) {

// Get the corresponding column name and convert the table name to lowercase

String dataColumnName = TABLE_TO_COLUMN.get(table);

String LowerCaseTable= table.toLowerCase();

// Create a new pipeline

Pipeline dataTransferPipeline=Pipeline.create(options);

// Delete existing data from the PostgreSQL table

dataTransferPipeline.apply("Create dummy PCollection", Create.of((Void) null))
                    .apply("Delete existing data", Jdbc10.<VOid>write()
                                    .withDataSourceConfiguration(Jdbc1O.DataSourceConfiguration.create(
                                            "org.postgresql.Driver", options.getPostgresUrl())
                                            .withUsername (postgresSecrets.getDbUser()).withPassword(postgresSecrets.getDbPassword()))

                            .withStatement("DELETE FROM what if."+ LowerCaseTable).withPreparedStatementSetter((element, query) -> {}));

//we need this extra dummy collection to make sure the delete operation is completed before we start reading from BigQuery

PCollection <Void> dummyCollection=dataTransferPipeline
        .apply("Create another dummy PCollection for Wait.on", Create.of((Void) null));

// Read data from the BigQuery table PCollection<Row> tableRows dataTransferPipeline

PCollection<Row> tableRows = dataTransferPipeline.apply("Read from BigQuery query",
        BigQueryI0.read((schemaAndRecord) -> SkuData.rowFromGenericRecord (schemaAndRecord.getRecord(), table))
                .withCoder (RowCoder.of(TABLE_TO_SCHEMA.get(table)))
                .fromQuery(String.format(TABLE_TO_SELECT_SQL.get(table), project, dataset))
                .usingStandardSql());

PCollection <SkuData> skuData = tableRows.apply(
        PythonExternalTransform.<PCollection<Row>, PCollection<KV<Long, byte[]>>>
from("__constructor__").withArgs(jsonToNumpy, TABLE TO PYTHON TRANSFORM.get(table))
                .withExtraPackages(List.of("Lz4"))).apply(
                        "Read KV and compress", MapElements.into(TypeDescriptor.of(SkuData.class))
                .via(row->{
                    if(row == null)
                        return null;
                }
                return new SkuData(row.getKey().intValue(),row,getValue());
})
);

    // Wait for the delete operation to complete before proceeding

    PCollection<SkuData> delayedTableRows =

            skuData.apply("Wait for delete operation", Wait.on(dummyCollection));

// Write the transformed data to the PostgreSQL table

    delayedTableRows.apply("WriteToPostgres", JdbcIO.<SkuData>write()

            .withDataSourceConfiguration(Jdbc10.DataSourceConfiguration.create("org.postgresql.Driver",
                            options.getPostgresUrl())
                    .withUsername (postgresSecrets.getDbUser())
                    .withPassword(postgresSecrets.getDbPassword()))
                    .withStatement("INSERT INTO what_if." + LowerCaseTable + " (sku," + dataColumnName + ") VALUES (?, ?)")
                    .withPreparedStatementSetter((JdbcI0.PreparedStatementSetter<SkuData>) (element, query) -> {
        assert element != null;
        query.setInt(1, element.getSku());
        query.setBinaryStream(2, new ByteArrayInputStream(element.getData()));

    }).withBatchSize(options.getBatchSize())); // Batch size for writing to PostgreSQL

// Run the pipeline and wait for it to finish before moving on to the next table

    dataTransferPipeline.run().waitUntilFinish();

}}}