package com.yug.constants;

/**
 * @author gy
 * @version 1.0
 * @description TODO
 * @date 2023/1/3 15:01
 */
public class Constants {

    /**
     * 本机IP
     * demo代码都是将任务提交到远程Spark集群上的，所以需要绑定本机IP
     */
    public static final String LOCALHOST_IP = "192.168.32.17";
    /**
     * Spark集群地址
     */
    public static final String SPARK_ADDRESS = "192.168.21.25:7077";
    /**
     * HDFS地址
     */
    public static final String HDFS_ADDRESS = "192.168.21.25:9000";
    /**
     * SQLServer地址
     */
    public static final String SQLSERVER_ADDRESS = "192.168.11.133:1433";
}
