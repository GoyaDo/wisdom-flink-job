package com.wisdom.flink.properties;

import lombok.Data;

/**
 * @author cj
 * @since 2022-10-08
 */
@Data
public class SqlManagerProperties {

    /**
     * 主机地址
     */
    private String hostName;

    /**
     * 端口号
     */
    private Integer port;

    /**
     * 数据库名称
     */
    private String[] databases;

    /**
     * 表名称
     */
    private String[] tables;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 密码
     */
    private String passWord;
}
