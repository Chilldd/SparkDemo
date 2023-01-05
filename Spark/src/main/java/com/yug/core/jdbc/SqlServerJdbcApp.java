package com.yug.core.jdbc;

import com.yug.constants.Constants;
import com.yug.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author gy
 * @version 1.0
 * @description 使用JDBC连接SqlServer
 * @date 2022/12/15 18:39
 */
public class SqlServerJdbcApp {
    public static void main(String[] args) {
        SparkUtils.setHadoopUser();
        SparkSession spark = SparkUtils.buildDefaultSparkSession("SQLServer JDBC");

        Properties properties = new Properties();
        properties.setProperty("user", "sa");
        properties.setProperty("password", "Password01!");

        spark.read()
                .jdbc(
                        String.format("jdbc:sqlserver://%s;DatabaseName=dmp_ods;encrypt=true;trustServerCertificate=true", Constants.SQLSERVER_ADDRESS),
                        "YuG.PipleLog",
                        properties)
                .createOrReplaceTempView("piplelog");

        spark.sql("select source_id,pipel_id,msg,type from piplelog limit 3")
                .show();

        spark.stop();
    }
}
