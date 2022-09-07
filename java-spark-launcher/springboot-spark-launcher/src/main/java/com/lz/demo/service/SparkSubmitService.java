package com.lz.demo.service;

import com.lz.demo.entity.SparkApplicationParam;
import com.lz.demo.vo.Result;

import java.io.IOException;

public interface SparkSubmitService {
    /**
     * 提交spark任务入口
     *
     * @param sparkAppParams spark任务运行所需参数
     * @return 结果
     * @throws IOException          io
     * @throws InterruptedException 线程等待中断异常
     */
    Result submitApplication(SparkApplicationParam sparkAppParams) throws IOException, InterruptedException;
}
