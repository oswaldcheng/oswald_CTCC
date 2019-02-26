package com.oswald.utils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 单例JdbcConnection
 *
 * @ClassName JdbcInstance
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
public class JdbcInstance {
    private static Connection connection = null;

    private JdbcInstance() {}

    public static Connection getInstance() {
        try {
            if (connection == null || connection.isClosed() || !connection.isValid(3)) {
                connection = JdbcUtil.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
