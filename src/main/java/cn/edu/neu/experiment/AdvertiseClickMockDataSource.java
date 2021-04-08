package cn.edu.neu.experiment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author 32098
 */
public class AdvertiseClickMockDataSource extends RichParallelSourceFunction<AdvertiseClickBean> {
    private boolean keepMock;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        keepMock = true;
    }

    @Override
    public void run(SourceContext<AdvertiseClickBean> sourceContext) throws Exception {
        List<String> provinceList = Arrays.asList("江西", "辽宁", "浙江", "广东", "湖南", "湖北", "吉林", "黑龙江", "福建");
        List<String> cityList = Arrays.asList("南昌","沈阳","杭州","广州","长沙","武汉","长春","哈尔滨","厦门");

        int len = provinceList.size();
        Random r = new Random();
        while (keepMock) {
            for(int i=0; i<r.nextInt(150); i++){
                int idx = r.nextInt(len);
                String aid = "Ad_" + r.nextInt(20);
                // 模拟数据延迟与乱序
                Long clickTime = System.currentTimeMillis() - r.nextInt(3)*1000;
                String clickUserId = "U" + r.nextInt(10);
                String clickUserProvince = provinceList.get(idx);
                String clickUserCity = cityList.get(idx);
                sourceContext.collect(new AdvertiseClickBean(aid, clickTime, clickUserId, clickUserProvince, clickUserCity));
            }
            Thread.sleep(6000);
        }
    }

    @Override
    public void cancel() {
        keepMock = false;
    }
}
