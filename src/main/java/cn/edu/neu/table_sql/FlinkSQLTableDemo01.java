package cn.edu.neu.table_sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 32098
 */
public class FlinkSQLTableDemo01 {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }

    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Default to be Blink Planner
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        /*
        Or >>>
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
         */

        // 2.Source
        DataStream<Order> orderA = env.fromCollection(
                Arrays.asList(
                    new Order(1L, "beer", 3),
                    new Order(1L, "diaper", 4),
                    new Order(3L, "rubber", 2)
                )
        );

        DataStream<Order> orderB = env.fromCollection(
                Arrays.asList(
                    new Order(2L, "pen", 3),
                    new Order(2L, "rubber", 3),
                    new Order(4L, "beer", 1)
                )
        );

        // 3.注册表
        // Convert DataStream to Table
        Table tableA = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        // Register DataStream as Table
        tEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));

        // 4.执行查询
        System.out.println(tableA);

        // union the two tables
        Table resultTable = tEnv.sqlQuery(
                "SELECT * FROM " + tableA + " WHERE amount > 2 " +
                        "UNION ALL " +
                        "SELECT * FROM OrderB WHERE amount < 2"
        );

        // 5.输出结果
        DataStream<Order> resultDS = tEnv.toAppendStream(resultTable, Order.class);
        resultDS.print();

        env.execute();
    }
}
