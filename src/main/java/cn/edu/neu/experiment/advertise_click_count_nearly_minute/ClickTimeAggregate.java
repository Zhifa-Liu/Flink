package cn.edu.neu.experiment.advertise_click_count_nearly_minute;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author 32098
 */
public class ClickTimeAggregate implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
    /**
     * 创建累加器
     * @return 返回累加器初始值 0
     */
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    /**
     * 点击次数往累加器加
     * @param in 输入
     * @param acc 当前的累加器值
     * @return 更新的累加器值
     */
    @Override
    public Long add(Tuple2<String, Long> in, Long acc) {
        return in.f1 + acc;
    }

    /**
     * 获取累加器的最终值
     * @param acc 累加器的最终值
     * @return 累加器的最终值
     */
    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    /**
     * 合并各个subTask的结果
     */
    @Override
    public Long merge(Long accA, Long accB) {
        return accA + accB;
    }
}