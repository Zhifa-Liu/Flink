package cn.edu.neu.experiment.advertise_blacklist;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author 32098
 */
public class MysqlSink extends RichSinkFunction<AdvertiseClickData> {
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://master:3306/user_advertise", "root", "Hive@2020");
    }

    @Override
    public void invoke(AdvertiseClickData value, Context context) throws Exception {
        PreparedStatement ps = conn.prepareStatement("select uid from black_list where uid=?");
        ps.setString(1, value.getClickUserId());
        ResultSet rs = ps.executeQuery();
        if(!rs.next()){
            String day = value.getClickTime();
            ps = conn.prepareStatement(
                    "select * from user_advertise where day=? and uid=? and aid=?"
            );
            ps.setString(1, day);
            ps.setString(2, value.getClickUserId());
            ps.setString(3, value.getAdvertiseId());
            rs = ps.executeQuery();
            if(rs.next()){
                PreparedStatement psA = conn.prepareStatement(
                        "update user_advertise set count = ? where day=? and uid=? and aid=?"
                );
                psA.setLong(1, value.getClickCount());
                psA.setString(2, day);
                psA.setString(3, value.getClickUserId());
                psA.setString(4, value.getAdvertiseId());
                psA.executeUpdate();
                psA.close();
            }else{
                PreparedStatement psB = conn.prepareStatement("insert into user_advertise(day,uid,aid,count) values (?,?,?,?)");
                psB.setString(1, day);
                psB.setString(2, value.getClickUserId());
                psB.setString(3, value.getAdvertiseId());
                psB.setLong(4, value.getClickCount());
                psB.executeUpdate();
                psB.close();
            }
            ps = conn.prepareStatement(
                    "select * from user_advertise where day=? and uid=? and aid=? and count>60"
            );
            ps.setString(1, day);
            ps.setString(2, value.getClickUserId());
            ps.setString(3, value.getAdvertiseId());
            rs = ps.executeQuery();
            if(rs.next()){
                PreparedStatement psC = conn.prepareStatement("insert into black_list(uid) value(?) on duplicate key update uid=?");
                psC.setString(1, value.getClickUserId());
                psC.setString(2, value.getClickUserId());
                psC.executeUpdate();
                psC.close();
            }
            ps.close();
        }
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}
