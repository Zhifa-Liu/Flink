package cn.edu.neu.experiment.province_city_ad_click_count;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author 32098
 */
public class MysqlSink extends RichSinkFunction<AdvertiseClickData>{
    private Connection conn = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://master:3306/user_advertise", "root", "Hive@2020");
        String sql = "insert into province_city_advertise(day,province,city,aid,count) values (?,?,?,?,?) on duplicate key update count=?";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void invoke(AdvertiseClickData value, Context context) throws Exception {
        ps.setString(1, value.getClickTime());
        ps.setString(2, value.getClickUserProvince());
        ps.setString(3, value.getClickUserCity());
        ps.setString(4, value.getAdvertiseId());
        ps.setInt(5, value.getClickCount());
        ps.setInt(6, value.getClickCount());
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}

