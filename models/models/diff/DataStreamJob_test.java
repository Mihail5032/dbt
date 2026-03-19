package ru.x5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import ru.x5.config.PropertiesHolder;
import ru.x5.factory.KafkaSourceFactory;
import ru.x5.factory.StreamExecutionEnvironmentFactory;
import ru.x5.process.RawDataProcessFunction;
import ru.x5.process.StreamSideOutputTag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataStreamJob {
    public static final String CATALOG = "raw_table";
    public static final String SCHEMA = "raw_table";
    private static final String UID_SOURCE = "kafka-source";
    private static final String UID_PROCESS = "raw-data-process";
    private final Map<String, TableIdentifier> tableMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        DataStreamJob dataStreamJob = new DataStreamJob();
        dataStreamJob.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.getStreamExecutionEnvironment();
        DataStreamSource<String> stream = env.fromSource(
                KafkaSourceFactory.buildKafkaSource(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source").setParallelism(10);

        PropertiesHolder config = PropertiesHolder.getInstance();

        // === HARDCODE S3 CONFIG FOR TEST ===
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.access.key", "");
        hadoopConf.set("fs.s3a.secret.key", "");

        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopConf.set("fs.s3a.region", "us-east-1");
        hadoopConf.set("hive.metastore.uris", "thrift://hms.s3-hms.svc.cluster.local:9083");
        hadoopConf.setBoolean("fs.s3a.impl.disable.cache", true);

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", "s3a://warehouse");
        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProps.put("s3.endpoint", "http://minio.minio.svc.cluster.local:9000");
        catalogProps.put("s3.path-style-access", "true");
        catalogProps.put("s3.access-key-id", "");
        catalogProps.put("s3.secret-access-key", "");
        catalogProps.put("client.region", "us-east-1");

        CatalogLoader catalogLoader = CatalogLoader.hive(CATALOG, hadoopConf, catalogProps);
        // === END HARDCODE ===

        Map<String, TableLoader> tableLoaders = new HashMap<>();
        Map<String, Schema> icebergSchemas = new HashMap<>();

        // RAW: только Transaction (нужна для toRowData + коррекции)
        tableMap.put("E1BPTRANSACTION", TableIdentifier.of(SCHEMA, "raw_bptransaction"));

        for (Map.Entry<String, TableIdentifier> entry : tableMap.entrySet()) {
            String segment = entry.getKey();
            TableIdentifier tableId = entry.getValue();
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, tableId);
            tableLoaders.put(segment, loader);
            Schema schema = getIcebergSchema(loader);
            icebergSchemas.put(segment, schema);
        }

        // PST: пишем в тестовую таблицу raw_bptransaction_test
        Map<String, TableIdentifier> pstTableMap = new HashMap<>();
        pstTableMap.put("PST_BPTRANSACTION", TableIdentifier.of(SCHEMA, "raw_bptransaction_test"));

        Map<String, Schema> pstSchemas = new HashMap<>();
        for (Map.Entry<String, TableIdentifier> entry : pstTableMap.entrySet()) {
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, entry.getValue());
            pstSchemas.put(entry.getKey(), getIcebergSchema(loader));
        }

        SingleOutputStreamOperator<RowData> rowData = stream
                .uid(UID_SOURCE)
                .process(new RawDataProcessFunction(icebergSchemas, pstSchemas))
                .setParallelism(10)
                .uid(UID_PROCESS);

        // PST sink — только Transaction в тестовую таблицу
        OutputTag<RowData> tag = StreamSideOutputTag.getTag("PST_BPTRANSACTION");
        DataStream<RowData> pstStream = rowData.getSideOutput(tag);
        TableLoader loader = TableLoader.fromCatalog(catalogLoader, pstTableMap.get("PST_BPTRANSACTION"));

        FlinkSink.forRowData(pstStream)
                .tableLoader(loader)
                .writeParallelism(1)
                .upsert(false)
                .uidPrefix("sink-pst_bptransaction_test")
                .set("write.target-file-size-bytes", "268435456")
                .append();

        env.execute("TEST: is_aligned_tran check");
        closeTableLoaders(tableLoaders);
        env.close();
    }

    private Schema getIcebergSchema(TableLoader tableLoaderDynamic) throws IOException {
        Table icebergTableDynamic;
        try (tableLoaderDynamic) {
            tableLoaderDynamic.open();
            icebergTableDynamic = tableLoaderDynamic.loadTable();
        }
        return icebergTableDynamic.schema();
    }

    private void closeTableLoaders(Map<String, TableLoader> tableLoaders) {
        tableLoaders.values().forEach(l -> {
            try {
                l.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
