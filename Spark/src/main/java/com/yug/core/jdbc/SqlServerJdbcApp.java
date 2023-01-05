package com.yug.core.jdbc;

import com.yug.constants.Constants;
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
        System.setProperty("HADOOP_USER_NAME", "root");

        SparkConf conf = new SparkConf()
                .setAppName("JDBC SQL")
                .setMaster(String.format("spark://%s", Constants.SPARK_ADDRESS))
                .set("spark.submit.deployMode", "client")
                .set("spark.executor.cores", "1")
                .set("spark.executor.memory", "1024M")
                .set("spark.driver.host", Constants.LOCALHOST_IP);

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

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
