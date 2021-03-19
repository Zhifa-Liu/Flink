package cn.edu.neu.batch_stream;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 32098
 *
 * Desc requirement:
 * 将scoreDS(学号, 学科, 成绩)中的数据和分布式缓存中的数据(学号,姓名)关联,得到这样格式的数据: (学生姓名,学科,成绩)
 *
 * Attention：
 * 广播变量是将变量分发到各个TaskManager节点的内存上，分布式缓存是将文件缓存到各个TaskManager节点上
 */
public class DistributedCache {
    public static void main(String[] args) throws Exception {
        // 1.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.source
        // 注意:先将本地资料中的distribute_cache_student文件上传到HDFS
        // a.注册分布式缓存文件
        env.registerCachedFile("data/input/distribute_cache_student", "distribute_cache_student");
        // env.registerCachedFile("hdfs://hostname:port/distribute_cache_student", "distribute_cache_student");

        DataSource<Tuple3<Integer, String, Integer>> scoreDS = env.fromCollection(
                Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86))
        );

        // 3.transformation
        // 将scoreDS(学号, 学科, 成绩)中的数据和分布式缓存中的数据(学号,姓名)关联,得到这样格式的数据: (学生姓名,学科,成绩)
        MapOperator<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>> result = scoreDS.map(
                new RichMapFunction<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>>() {
                    // 定义一集合用来存储(学号,姓名)
                    final Map<Integer, String> studentMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // b.加载分布式缓存文件
                        File file = getRuntimeContext().getDistributedCache().getFile("distribute_cache_student");
                        List<String> studentList = FileUtils.readLines(file);
                        for (String str : studentList) {
                            String[] arr = str.split(",");
                            studentMap.put(Integer.parseInt(arr[0]), arr[1]);
                        }
                    }

                    @Override
                    public Tuple3<String, String, Integer> map(Tuple3<Integer, String, Integer> value) throws Exception {
                        // c.使用分布式缓存文件中的数据
                        Integer stuId = value.f0;
                        String stuName = studentMap.getOrDefault(stuId, "");
                        // return (name,subject,score)
                        return Tuple3.of(stuName, value.f1, value.f2);
                    }
                }
        );

        // 4.Sink
        result.print();
    }
}


