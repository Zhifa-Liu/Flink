package cn.edu.neu.table_sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;


/**
 * @author 32098
 *
 * Desc requirement:
 * 每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 *
 * Sql style
 */
public class FlinkSQLTableDemo04 {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    public static void main(String[] args) throws Exception {
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2.Source
        DataStreamSource<Order> orderDs  = env.addSource(
            new RichSourceFunction<Order>() {
                private Boolean isRunning = true;

                @Override
                public void run(SourceContext<Order> ctx) throws Exception {
                    Random random = new Random();
                    while (isRunning) {
                        Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                        TimeUnit.SECONDS.sleep(1);
                        ctx.collect(order);
                    }
                }

                @Override
                public void cancel() {
                    isRunning = false;
                }
            }
        );

        // 3.Transformation
        DataStream<Order> watermakerDs = orderDs.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((event, timestamp) -> event.getCreateTime())
        );

        // 4.注册表
        tEnv.createTemporaryView(
                "t_order",
                watermakerDs,
                $("orderId"), $("userId"), $("money"), $("createTime").rowtime()
        );

        //5.执行SQL
        String sql =
                "select " +
                "userId," +
                "count(*) as totalCount," +
                "max(money) as maxMoney," +
                "min(money) as minMoney " +
                "from t_order " +
                "group by userId," +
                "tumble(createTime, interval '5' second)"; // 滚动窗口：5s滚动一次

        /*
         TUMBLE(timeColumn, interval 'int' DAY/HOUR/MINUTE/SECOND)
         HOP(timeColumn, slide, window_length)
         SESSION(timeColumn, gap) // 窗口数据非活跃周期的时长
         */

        Table resultTable = tEnv.sqlQuery(sql);

        // 6.Sink
        // 将SQL的执行结果转换成DataStream再打印出来
        // toAppendStream → 将计算后的数据append到结果DataStream中去
        // toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上插入(true means insert)或是删除(false means delete)
        DataStream<Tuple2<Boolean, Row>> resultDs = tEnv.toRetractStream(resultTable, Row.class);
        resultDs.print();

        env.execute();
    }
}

