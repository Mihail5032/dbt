package ru;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import ru.Processors.AdapterRunProcessorFunction;

import static ru.Utils.AwsUtils.*;


@Slf4j
public class AdapterRunH2hStream {

    public static void main(String[] args) throws Exception {

      EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
      final StreamExecutionEnvironment env = getStreamExecutionEnvironment();
      final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
      Configuration hadoopConf = getEntriesHMS();

      CatalogLoader catalogLoader = CatalogLoader.hive("ice_baby", hadoopConf, getEntriesS3());

      TableIdentifier tableIdentifier = TableIdentifier.of("h2h", "h2h_result");
      TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader,tableIdentifier);
      Table icebergTable =  catalogLoader.loadCatalog().loadTable(tableIdentifier);

        // Check the current snapshot ID
      long currentSnapshotId = icebergTable.currentSnapshot().snapshotId();
      log.info("Current Snapshot ID: " + currentSnapshotId);
      DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();
      org.apache.flink.table.api.Table flinkTable = tableEnv.fromDataStream(stream);
      tableEnv.createTemporaryView("h2h_result",flinkTable);
      org.apache.flink.table.api.Table resultTable = tableEnv.sqlQuery("SELECT * FROM h2h_result");

      DataStream<String> resultStream = tableEnv.toDataStream(resultTable)
                .keyBy(row -> Tuple3.of(
                        (String)row.getField("werks"),
                        (String)row.getField("bank"),
                        (String)row.getField("bdate")
                        ),
                        Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .process(new AdapterRunProcessorFunction());
      resultStream.print();
      env.execute("H2h-Stream Job");
    }
}

