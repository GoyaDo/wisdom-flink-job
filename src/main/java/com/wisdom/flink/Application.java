/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wisdom.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.wisdom.flink.serialization.MyDebeziumDeserializationSchema;
import com.wisdom.flink.sink.OracleSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

/**
 * @author cj
 * @since 2022-10-08
 */
public class Application {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> build = MySqlSource.<String>builder()
                .hostname("172.168.10.194")
                .port(3306)
                //数据库用逗号分隔,可以配置多数据库
                .databaseList("project_center")
                //库.表 逗号分隔,可以配置多张表
                .tableList("project_center.T_APP_USER_REAL_NAME,project_center.T_FAMILY_MEMBER")
                .username("root")
                .password("root")
                //自定义序列化器
                .deserializer(new MyDebeziumDeserializationSchema())
                //监听规则
                .startupOptions(StartupOptions.initial())
                .build();

        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        env.fromSource(build, WatermarkStrategy.noWatermarks(), "MySql Source")
                .addSink(new OracleSink());

        // Execute program, beginning computation.
        env.execute("Flink Java API Skeleton");
    }
}
