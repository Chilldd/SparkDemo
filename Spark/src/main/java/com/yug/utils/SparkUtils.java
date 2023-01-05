package com.yug.utils;

import com.yug.constants.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author gy
 * @version 1.0
 * @description TODO
 * @date 2023/1/5 16:45
 */
public class SparkUtils {

    public static void setHadoopUser() {
        System.setProperty("HADOOP_USER_NAME", "root");
    }

    public static SparkSession buildDefaultSparkSession(String jobName) {
        return SparkSession.builder()
                .config(buildDefaultSparkConf(jobName))
                .getOrCreate();
    }

    public static SparkSession buildHiveSparkSession(String jobName) {
        return SparkSession.builder()
                .config(buildDefaultSparkConf(jobName))
                .enableHiveSupport()
                .getOrCreate();
    }

    public static SparkConf buildDefaultSparkConf(String jobName) {
        return new SparkConf()
                .setAppName(jobName)
                .setMaster(String.format("spark://%s", Constants.SPARK_ADDRESS))
                .set("spark.submit.deployMode", "client")
                .set("spark.executor.cores", "1")
                .set("spark.executor.memory", "1024M")
                .set("spark.driver.host", Constants.LOCALHOST_IP);
    }
}
