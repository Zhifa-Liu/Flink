package cn.edu.neu.batch_stream_api.source.SelfDefinedSource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Desc
 *需求
 * 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 * 要求:
 * - 随机生成订单ID(UUID)
 * - 随机生成用户ID
 * - 随机生成订单金额(0-100)
 * - 时间戳为当前系统时间
 *
 * API
 * Flink 数据源接口--->
 * SourceFunction:非并行数据源(并行度只能=1)
 * RichSourceFunction:多功能非并行数据源(并行度只能=1)
 * ParallelSourceFunction:并行数据源(并行度能够>=1)
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
 *
 * @author 32098
 */
public class OrderMockSource {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    public static class OrderSource extends RichParallelSourceFunction<Order> {
        private boolean flag = true;

        @Override
        public void run(SourceContext<Order> sourceContext) throws Exception {
            Random random = new Random();
            while (flag) {
                Thread.sleep(1000);
                String id = java.util.UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                sourceContext.collect(new Order(id, userId, money, createTime));
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 准备环境
        // 1.1 创建流式执行环境实例对象
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置运行类型
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2. 定义数据源
        DataStreamSource<Order> orderDs = environment.addSource(new OrderSource()).setParallelism(2);

        // 3. transformation

        // 4. sink
        orderDs.print();

        // 5. execute
        environment.execute();
    }

}
