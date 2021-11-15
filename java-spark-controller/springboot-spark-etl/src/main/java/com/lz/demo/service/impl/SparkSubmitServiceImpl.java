package com.lz.demo.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lz.demo.entity.SparkApplicationParam;
import com.lz.demo.service.SparkSubmitService;
import com.lz.demo.util.HttpUtil;
import com.lz.demo.vo.Result;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Service
public class SparkSubmitServiceImpl implements SparkSubmitService {
    private static final Logger log = LoggerFactory.getLogger(SparkSubmitServiceImpl.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Value("${spark.driver.name:localhost}")
    private String driverName;

    @Override
    public Result submitApplication(SparkApplicationParam sparkAppParams, String... otherParams) throws IOException, InterruptedException {
        log.info("spark任务传入参数：{}", sparkAppParams.toString());
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Map<String, String> confParams = sparkAppParams.getOtherConfParams();
        SparkLauncher launcher = new SparkLauncher()
                // 待提交给spark集群处理的spark application jar所在路径
                .setAppResource(sparkAppParams.getJarPath())
                // 设置spark application主类
                .setMainClass(sparkAppParams.getMainClass())
                // 设置该spark application的master
                .setMaster(sparkAppParams.getMaster())
                // 设置集群部署模式
                .setDeployMode(sparkAppParams.getDeployMode())
                // spark-submit的详细报告
                .setVerbose(true)
                // 设置用于执行spark application的spark集群分配的executor内存等参数
                .setConf("spark.driver.memory", sparkAppParams.getDriverMemory())
                .setConf("spark.executor.memory", sparkAppParams.getExecutorMemory())
                .setConf("spark.executor.cores", sparkAppParams.getExecutorCores());
        if (confParams != null && confParams.size() != 0) {
            log.info("开始设置spark job运行参数:{}", mapper.writeValueAsString(confParams));
            for (Map.Entry<String, String> conf : confParams.entrySet()) {
                log.info("{}:{}", conf.getKey(), conf.getValue());
                launcher.setConf(conf.getKey(), conf.getValue());
            }
        }
        if (otherParams.length != 0) {
            log.info("开始设置spark job参数:{}", Arrays.toString(otherParams));
            // 添加传递给spark application main方法的参数
            launcher.addAppArgs(otherParams);
        }
        log.info("参数设置完成，开始提交spark任务");
        SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                // application的状态（UNKNOWN、SUBMITTED、RUNNING、FINISHED、FAILED、KILLED、 LOST）
                if (sparkAppHandle.getState().isFinal()) {
                    countDownLatch.countDown();
                }
                log.info("stateChanged:{}", sparkAppHandle.getState().toString());
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                log.info("infoChanged:{}", sparkAppHandle.getState().toString());
            }
        });
        log.info("The task is executing, please wait ....");
        // 线程等待任务结束
        countDownLatch.await();
        log.info("The task is finished!");
        // 通过spark原生的监测api获取执行结果信息，需要在spark-defaults.conf、spark-env.sh进行相应的配置
        String restUrl;
        try {
            restUrl = "http://" + driverName + ":18080/api/v1/applications/" + handle.getAppId();
            log.info("访问application运算结果，url:{}", restUrl);
            return Result.success(HttpUtil.httpGet(restUrl, null));
        } catch (Exception e) {
            log.info("18080端口异常，请确保spark history server服务已开启");
            return Result.err(1, "history server is not start");
        }
    }
}
