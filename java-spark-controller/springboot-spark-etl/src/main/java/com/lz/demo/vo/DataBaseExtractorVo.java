package com.lz.demo.vo;

import com.lz.demo.entity.SparkApplicationParam;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataBaseExtractorVo extends SparkApplicationParam {
    /**
     * 数据库连接地址
     */
    private String url;
    /**
     * 数据库连接账号
     */
    private String username;
    /**
     * 数据库密码
     */
    private String password;
    /**
     * 指定的表名
     */
    private String table;
    /**
     * 目标文件类型
     */
    private String targetFileType;
    /**
     * 目标文件保存路径
     */
    private String targetFilePath;
}