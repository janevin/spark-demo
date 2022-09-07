package com.lz.demo.controller;

import com.lz.demo.entity.SparkApplicationParam;
import com.lz.demo.service.SparkSubmitService;
import com.lz.demo.vo.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.io.IOException;

@Slf4j
@Controller
public class SparkController {
    @Resource
    private SparkSubmitService sparkSubmitService;

    /**
     * 调用service进行远程提交spark任务
     *
     * @param param 页面参数
     * @return 执行结果
     */
    @ResponseBody
    @PostMapping("/submit")
    public Result sparkSubmit(@RequestBody SparkApplicationParam param) {
        try {
            return sparkSubmitService.submitApplication(param);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            log.error("执行出错：{}", e.getMessage());
            return Result.err(500, e.getMessage());
        }
    }
}
