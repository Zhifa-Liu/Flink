package cn.edu.neu.advance_api.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author 32098
 *
 * Desc requirement:
 * 设置会话超时时间为10s，10s内没有数据到来，则触发上个窗口的计算
 *
 * 会话窗口：窗口数据没有固定的大小，根据窗口数据活跃程度划分窗口，窗口数据无叠加
 */
public class SessionWindow {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo {
        // 信号灯id
        private String sensorId;
        // 通过该信号灯的车的数量
        private Integer count;
    }

    public static void main(String[] args) throws Exception {
        // 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.Source
        DataStreamSource<String> socketDs = env.socketTextStream("master", 9999);

        // 3.Transformation
        SingleOutputStreamOperator<CartInfo> cartInfoDs = socketDs.map(
                new MapFunction<String, CartInfo>() {
                    @Override
                    public CartInfo map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new CartInfo(arr[0], Integer.parseInt(arr[1]));
                    }
                }
        );

        // 会话窗口：
        // ProcessingTimeSessionWindows
        SingleOutputStreamOperator<CartInfo> result = cartInfoDs.keyBy(CartInfo::getSensorId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .sum("count");

        // 4.sink
        result.print();

        // 5.execute
        env.execute();
    }
}
