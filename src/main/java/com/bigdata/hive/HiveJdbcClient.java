package com.bigdata.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
  //1.2.0后不需要添加，用户定义驱动名称
  //private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
//      try {
//      Class.forName(driverName);
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//      System.exit(1);
//    }
    //replace "hive" here with the name of the user the queries should run as
    Connection con = DriverManager.getConnection("jdbc:hive2://ghl01:10000/haozu", "root", "123456");
    //创建一条语句
    Statement stmt = con.createStatement();
    //定义表名称
    //String tableName = "testHiveDriverTable";
    //实例化
    //stmt.execute("drop table if exists " + tableName);
    //stmt.execute("create table " + tableName + " (key int, value string)");
    // show tables
    String sql = "select * from student";    //不能使用分号
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1)+"\t"+res.getString(2));
    }
  }
}
