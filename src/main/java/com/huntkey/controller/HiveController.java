package com.huntkey.controller;

import org.json.JSONObject;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;

/**
 * Created by liuwens on 2017/7/26.
 */
@RestController
@RequestMapping("/hive")
public class HiveController
{
    //连接hive的URL hive1.2.1版本需要的是jdbc:hive2，而不是 jdbc:hive
    private static final String URLHIVE = "jdbc:hive2://192.168.13.32:10000/default";
    private Connection connection = null;

    public Connection getHiveConnection() {
        if (null == connection) {
            synchronized (HiveController.class) {
                if (null == connection) {
                    try {
                        //hive的jdbc驱动类, 不能多hadoop这个路径
                        Class.forName("org.apache.hive.jdbc.HiveDriver");

                        //登录linux的用户名  一般会给权限大一点的用户，否则无法进行事务形操作
                        connection = DriverManager.getConnection(URLHIVE, "root", "Huntkey12");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }

    @RequestMapping("/key/{keyValue}")
    public String queryByKey(@PathVariable("keyValue") String keyValue)
    {
        JSONObject obj = null;
        try
        {
            getHiveConnection();

            String sql = "select * from hive_t_hk_dept where key = '" + keyValue + "'";

            obj = query(sql);
        } catch (SQLException e)
        {
            e.printStackTrace();
        }

        return obj.toString();
    }

    @RequestMapping("/test")
    public String test()
    {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getHiveConnection();
            stmt = conn.createStatement();

            selectData(stmt, "hive_t_hk_dept");
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            try {
                if (stmt != null) {
                    stmt.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        return "dsfsdf";
    }

    private void selectData(Statement stmt, String tableName)
            throws SQLException {
        String sql = "select * from " + tableName;
        System.out.println("Running:" + sql);
        ResultSet res = stmt.executeQuery(sql);
        System.out.println("执行 select * query 运行结果:");
        while (res.next())
        {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }



    private JSONObject query(String sql) throws SQLException
    {
        JSONObject obj = new JSONObject();

        Statement stmt = connection.createStatement();

        ResultSet res = stmt.executeQuery(sql);
        System.out.println("执行 select * query 运行结果:");

        while (res.next())
        {
            obj.put("key", res.getString("key"));

            obj.put("dept_code_0", res.getString("dept_code_0"));
        }

        return obj;
    }


}
