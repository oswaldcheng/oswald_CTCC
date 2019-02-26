package com.oswald.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ClassName ConnectionInstance
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class ConnectionInstance {
    private static Connection conn;

    /**
     * 返回链接
     *
     * @param conf
     * @return
     */
    public static synchronized Connection getConnection(Configuration conf) {
        try {
            if (conn == null || conn.isClosed()) {
                conn = ConnectionFactory.createConnection(conf);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
