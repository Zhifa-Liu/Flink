package cn.edu.neu.batch_stream_api.source.SelfDefinedSource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * @author 32098
 */
public class MysqlSource {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    public static class MySQLSource extends RichParallelSourceFunction<Student> {
        private Connection conn = null;
        private PreparedStatement ps = null;
        private boolean flag = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            DriverManager.getConnection("jdbc:mysql://master:3306/flink", "root", "Hive@2020");
            String sql = "select id,name,age from student";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Student> sourceContext) throws Exception {
            while (flag) {
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    sourceContext.collect(new Student(id, name, age));
                }
                TimeUnit.SECONDS.sleep(5);
            }
        }

        @Override
        public void cancel() {
            if (conn != null) {
                try {
                    conn.close();
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.Source
        DataStream<Student> studentDS = env.addSource(new MySQLSource()).setParallelism(1);

        //3.Transformation
        //4.Sink
        studentDS.print();

        //5.execute
        env.execute();
    }
}

/*
CREATE DATABASE flink;

CREATE TABLE `student` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(255) DEFAULT NULL,
    `age` int(11) DEFAULT NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET=utf8;

INSERT INTO `student` VALUES ('1', 'jack', '18');
INSERT INTO `student` VALUES ('2', 'tom', '19');
INSERT INTO `student` VALUES ('3', 'rose', '20');
INSERT INTO `student` VALUES ('4', 'tom', '19');
INSERT INTO `student` VALUES ('5', 'jack', '18');
INSERT INTO `student` VALUES ('6', 'rose', '20');
 */