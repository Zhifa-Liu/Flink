package cn.edu.neu.table_sql;

// Flink\Blink Streaming Query
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
// Flink Batch Query
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
// Blink Batch Query
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author 32098
 */
public class API {
    public static void main(String[] args) {
        // Flink Streaming Query
        StreamExecutionEnvironment flinkSteamingEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings flinkStreamingSetting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment flinkStreamingTableEnv = StreamTableEnvironment.create(flinkSteamingEnv, flinkStreamingSetting);

        // Flink Batch Query
        ExecutionEnvironment flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment flinlBatchTableEnv = BatchTableEnvironment.create(flinkBatchEnv);

        // Blink Streaming Query
        StreamExecutionEnvironment blinkStreamingEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamingSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamingTableEnv = StreamTableEnvironment.create(blinkStreamingEnv, blinkStreamingSettings);

        // Blink Batch Query
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSetting);
    }
}
