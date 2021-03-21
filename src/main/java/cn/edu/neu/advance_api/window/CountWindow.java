package cn.edu.neu.advance_api.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 32098
 *
 * Desc requirement：
 * 数据为（信号灯编号，通过该信号灯的车的数量）：
 * 9,3
 * 9,2
 * 9,7
 * 4,9
 * 2,6
 * 1,5
 * 2,3
 * 5,7
 * 5,4
 * 通过：nc -lk 9999 输入
 *
 * 基于数量的滑动窗口：统计在最近6条消息中,通过各信号灯的汽车数量,相同的信号灯编号(key)每出现3次进行统计
 * 基于数量的滚动窗口：统计在最近6条消息中,通过各信号灯的汽车数量,相同的信号灯编号(key)每出现6次进行统计
 */
public class CountWindow {
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

        // 基于数量的滚动窗口
        SingleOutputStreamOperator<CartInfo> tumblingResult = cartInfoDs.keyBy(CartInfo::getSensorId).countWindow(6L).sum("count");

        // 基于数量的滑动窗口
        SingleOutputStreamOperator<CartInfo> slidingResult = cartInfoDs.keyBy(CartInfo::getSensorId).countWindow(6L, 3L).sum("count");

        // 4.sink
        tumblingResult.print("Tumbling>>>");
        slidingResult.print(" Sliding>>>");

        // 5.execute
        env.execute();
    }
}


/*
paste
9,2
9,7
4,9
2,6
1,5
2,3
5,7
5,4
enter by self
6,1
6,2
8,1
8,2
7,1
7,3
7,2
9,1
9,1
9,1
9,1

print:
 Sliding>>>:8> CountWindow.CartInfo(sensorId=7, count=6)
 Sliding>>>:7> CountWindow.CartInfo(sensorId=9, count=10)
Tumbling>>>:7> CountWindow.CartInfo(sensorId=9, count=13)
 Sliding>>>:7> CountWindow.CartInfo(sensorId=9, count=13)
 */