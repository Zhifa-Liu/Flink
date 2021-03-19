package cn.edu.neu.batch_stream.broadcast_var;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 32098
 *
 * Desc requirement：
 * Broadcast studentDS(id,name) to memory of every TaskManager，
 * and then use scoreDS(id,subject,score) join it to get (name,subject,score)
 *
 * 注意：
 * 1.广播变量是要把dataset广播到内存中，所以广播的数据量不能太大
 * 2.广播变量的值不可修改，这样才能确保每个节点获取到的值都是一致的
 * 3.如果不使用广播，每一个Task都会拷贝一份数据集，造成内存资源浪费
 */
public class UseBroadCast {
    public static void main(String[] args) throws Exception {
        // 1.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.source
        // studentDS(id,name)
        DataSource<Tuple2<Integer, String>> studentDs = env.fromCollection(
                Arrays.asList(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"), Tuple2.of(3, "王五"))
        );
        // scoreDS(id,subject,score)
        DataSource<Tuple3<Integer, String, Integer>> scoreDs = env.fromCollection(
                Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86))
        );

        // 3.transformation
        MapOperator<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>> result = scoreDs.map(
                new RichMapFunction<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>>() {
                    // 定义一集合用来存储(学号,姓名)
                    final Map<Integer, String> studentMap = new HashMap<>();

                    // open() 一般用来初始化资源，每个subtask任务只被调用一次
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // b.获取广播数据
                        List<Tuple2<Integer, String>> studentList = getRuntimeContext().getBroadcastVariable("studentInfo");
                        for (Tuple2<Integer, String> tuple : studentList) {
                            studentMap.put(tuple.f0, tuple.f1);
                        }
                        // or
                        // studentMap = studentList.stream().collect(Collectors.toMap(t -> t.f0, t -> t.f1));
                    }

                    @Override
                    public Tuple3<String, String, Integer> map(Tuple3<Integer, String, Integer> value) throws Exception {
                        // c.使用广播数据
                        Integer stuId = value.f0;
                        String stuName = studentMap.getOrDefault(stuId, "");
                        // return (name,subject,score)
                        return Tuple3.of(stuName, value.f1, value.f2);
                    }
                    // a.广播数据到各个TaskManager
                }
        ).withBroadcastSet(studentDs, "studentInfo");

        // 4.sink
        result.print();

        // 5.execute
        env.execute(); // java.lang.RuntimeException: No new data sinks have been defined since the last execution ==> result.print()
    }
}
