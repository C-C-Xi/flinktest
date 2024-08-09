package spendreport;

import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Data
public class moneyReport {
    public static void main(String[] args) throws Exception {
        // 1.创建Flink-MySQL-CDC的Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("tapout_pro") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("tapout_pro.shop","tapout_pro.shop_exchange") // 设置捕获的表
                .username("root")
                .password("mysql.2019")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(4)
                .print().setParallelism(1); // 设置 sink 节点并行度为 1

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
