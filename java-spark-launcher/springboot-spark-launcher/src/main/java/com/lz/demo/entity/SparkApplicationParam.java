package com.lz.demo.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class SparkApplicationParam {
    /**
     * MASTER_URL 如spark://host:port, mesos://host:port, yarn,  yarn-cluster,yarn-client, local
     */
    private String master;

    /**
     * DEPLOY_MODE client或者cluster，默认是client
     */
    private String deployMode;

    /**
     * 任务的主类
     */
    private String mainClass;

    /**
     * 应用程序的名称
     */
    private String appName;

    /**
     * jar包路径
     */
    private String jars;

    /**
     * 应用程序包
     */
    private String appResource;

    /**
     * file路径
     */
    private String files;

    /**
     * py-file路径
     */
    private String pyFiles;

    /**
     * 打印debug信息
     */
    private boolean verbose;

    /**
     * 其他配置：传递给spark job的参数
     */
    private Map<String, String> conf;

    /**
     * 应用程序参数
     */
    private List<String> appArgs;
}
