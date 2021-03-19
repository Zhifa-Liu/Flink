package cn.edu.neu.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Desc
 * 需求:使用Flink完成WordCount-DataSet
 * 编码步骤
 * 1.准备环境-env
 * 2.准备数据-source
 * 3.处理数据-transformation
 * 4.输出结果-sink
 * 5.触发执行-execute//如果有print,DataSet不需要调用execute,DataStream需要调用execute
 * @author 32098
 */
public class WordCountDataSet {
    public static void main(String[] args) throws Exception {
        //老版本的批处理API如下,但已经不推荐使用了
        //1.准备环境-env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.准备数据-source
        DataSet<String> lineDS = env.fromElements("nhbd hadoop spark","nhbd hadoop spark","nhbd hadoop","nhbd");
        //3.处理数据-transformation
        //3.1每一行数据按照空格切分成一个个的单词组成一个集合
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T value, Collector<O> out) throws Exception;
        }
         */
        DataSet<String> wordsDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value就是一行行的数据
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);//将切割处理的一个个的单词收集起来并返回
                }
            }
        });
        //3.2对集合中的每个单词记为1
        /*
        public interface MapFunction<T, O> extends Function, Serializable {
            O map(T value) throws Exception;
        }
         */
        DataSet<Tuple2<String, Integer>> wordAndOnesDS = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                //value就是进来一个个的单词
                return Tuple2.of(value, 1);
            }
        });

        //3.3对数据按照单词(key)进行分组
        //0表示按照tuple中的索引为0的字段,也就是key(单词)进行分组
        UnsortedGrouping<Tuple2<String, Integer>> groupedDS = wordAndOnesDS.groupBy(0);

        //3.4对各个组内的数据按照数量(value)进行聚合就是求sum
        //1表示按照tuple中的索引为1的字段也就是按照数量进行聚合累加!
        DataSet<Tuple2<String, Integer>> aggResult = groupedDS.sum(1);

        //3.5排序
        DataSet<Tuple2<String, Integer>> result = aggResult.sortPartition(1, Order.DESCENDING).setParallelism(1);

        //4.输出结果-sink
        result.print();

        //5.触发执行-execute//如果有print,DataSet不需要调用execute,DataStream需要调用execute
        //env.execute();//'execute()', 'count()', 'collect()', or 'print()'.
    }
}

