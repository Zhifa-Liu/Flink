package cn.edu.neu.batch_stream.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 32098
 */
public class FileBasedSource {
    public static void main(String[] args) {
        // 1. 准备环境
        // 1.1 创建流式执行环境实例对象
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置运行类型
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 2. 定义数据源
        // 2.1 本地文件
        DataStream<String> dataStreamA = environment.readTextFile("./data/words.txt");
        // 2.2 本地文件夹
        DataStream<String> dataStreamB = environment.readTextFile("./data/words");
        // 2.3 HDFS 文件 or 文件夹
        // DataStream<String> dataStreamC = environment.readTextFile("hdfs://master:9000//flink/example/wordcount/input");
        // 2.4 压缩文件
        // DataStream<String> dataStreamD = environment.readTextFile("./data/words.gz");

        // 3. Transformation: 略

        // 4. Sink
        dataStreamA.print("A");
        dataStreamB.print("B");
        // dataStreamC.print("C");
        // dataStreamD.print("D");

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
