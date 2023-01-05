package com.yug.core.hdfs;

import com.yug.constants.Constants;
import com.yug.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

/**
 * @author gy
 * @version 1.0
 * @description 读取HDFS上Parquet类型的文件
 * @date 2023/1/3 15:37
 */
public class ReadParquet {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkSession spark = SparkUtils.buildDefaultSparkSession("Read HDFS Parquet File");

        spark.read()
                .parquet(String.format("hdfs://%s/spark/parquet/ods_asddd/*", Constants.HDFS_ADDRESS))
                .createOrReplaceTempView("ods_asddd");
        spark.sql("select * from ods_asddd")
                .show();

        spark.stop();
    }
}
