package flinkcdc.tidb.test;


import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import com.ververica.cdc.connectors.tidb.table.TiDBTableSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.util.Collector;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.TiDBConvertException;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {


        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //2.通过FlinkCDC构建SourceFunction并读取数据

        SourceFunction<String> tidbSource =
                TiDBSource.<String>builder()
                        .database("hs_count") // set captured database
                        .tableName("hs_wide_new") // set captured table
                        .tiConf(
                                TDBSourceOptions.getTiConfiguration(
                                        "10.102.6.78:12379", new HashMap<>()))
                        .snapshotEventDeserializer(
                                new TiKVSnapshotEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Kvrpcpb.KvPair record, Collector<String> out)
                                            throws Exception {
                                        out.collect(record.toString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })
                        .changeEventDeserializer(
                                new TiKVChangeEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Cdcpb.Event.Row record, Collector<String> out)
                                            throws Exception {

                                        out.collect(record.getValue().toString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                }).startupOptions(StartupOptions.latest())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        // env.enableCheckpointing(3000);
        env.addSource(tidbSource).print().setParallelism(1);

        env.execute("Print TiDB Snapshot + Binlog");


    }

}
