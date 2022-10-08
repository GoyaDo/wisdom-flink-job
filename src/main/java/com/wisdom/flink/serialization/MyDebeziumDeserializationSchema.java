package com.wisdom.flink.serialization;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;

import java.util.List;
import java.util.Objects;

/**
 * 自定义序列化
 * @author cj
 * @since 2022-09-30
 */
public class MyDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String>  {

    /**
     * {
     *     "db":"",
     *     "tableName":"",
     *     "before":{"id":"","name":""},
     *     "after":{"id":"","name":""},
     *     "op":""
     * }
     * @param sourceRecord 行数据
     * @param collector 输出数据
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 创建Json对象用于封装结果数据
        JSONObject result = new JSONObject();
        // 获取库名、表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db",fields[1]);
        result.put("tableName",fields[2]);

        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//        if (StringUtils.equals(operation.toString(),"READ")){
//            return;
//        }
        result.put("op",operation);


        // 获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (Objects.nonNull(before)){
            // 获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                beforeJson.put(field.name(),before.get(field));
            }
        }
        result.put("before",beforeJson);

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (Objects.nonNull(after)){
            // 获取列信息
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                afterJson.put(field.name(),after.get(field));
            }
        }
        result.put("after",afterJson);

        //  输出数据
        collector.collect(result.toString());
    }

    /**
     * 获取类型
     * @return
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
