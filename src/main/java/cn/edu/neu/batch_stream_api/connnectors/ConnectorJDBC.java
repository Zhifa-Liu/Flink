package cn.edu.neu.batch_stream_api.connnectors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 32098
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/connectors/jdbc.html
 */
public class ConnectorJDBC {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static void main(String[] args) {
        // 1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. source
        env.fromElements(new Student(null, "tonyma", 18))
                // 3. transformation
                // 4. sink
        .addSink(JdbcSink.sink(
                "INSERT INTO `student` (`id`, `name`, `age`) VALUES (null, ?, ?)",
                (ps, s) -> {
                    ps.setString(1, s.getName());
                    ps.setInt(2, s.getAge());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://master:3306/flink")
                        .withUsername("root")
                        .withPassword("Hive@2020")
                        .withDriverName("com.mysql.jdbc.Driver").build()

                ));
        // 5. execute
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
