package com.wisdom.flink.sink;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.Session;
import cn.hutool.json.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;
import java.util.List;


/**
 * @author cj
 * @since 2022-09-30
 */
@Slf4j
public class OracleSink extends RichSinkFunction<String> {

    private Session session;

    private Session getSession() {
        //默认数据源
        return Session.create();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.session = getSession();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        super.invoke(value, context);
        log.info("数据保存!!!");
        JSONObject dbResult = new JSONObject(value);
        // 数据库名称
        String db = dbResult.getStr("db");
        // 表名
        String tableName = dbResult.getStr("tableName");
        // 操作名称
        String op = dbResult.getStr("op");
        JSONObject before = dbResult.getJSONObject("before");
        JSONObject after = dbResult.getJSONObject("after");
        if (StringUtils.equals(db, "project_center")) {
            Entity entity = null;
            switch (tableName) {
                case "T_APP_USER_REAL_NAME": {
                    entity = new Entity("T_APP_USER_REAL_NAME_PUSH");
                    break;
                }
                case "T_FAMILY_MEMBER": {
                    entity = new Entity("T_FAMILY_MEMBER_PUSH");
                    break;
                }
                default:
                    entity = new Entity();
                    break;
            }
            entity.parseBean(after, true, false);
            entity.set("DEL_FLAG", after.getStr("DELETED"));
            entity.remove("DELETED");
            entity.set("CREATE_TIME", after.getDate("CREATE_TIME"));
            entity.set("UPDATE_TIME", after.getDate("UPDATE_TIME"));
            try {
                session.beginTransaction();
                if (StringUtils.equals(op, "INSERT")) {
                    session.insert(entity);
                } else if (StringUtils.equals(op, "DELETE")) {
                    session.del(entity);
                } else if (StringUtils.equals(op, "UPDATE")) {
                    session.insertOrUpdate(entity);
                } else if (StringUtils.equals(op,"READ")) {
                    List<Entity> all = Db.use().findAll(Entity.create(entity.getTableName()).set("ID", entity.getStr("ID")));
                    if (CollUtil.isEmpty(all)){
                        session.insert(entity);
                    }
                }
                session.commit();
            } catch (SQLException e) {
                session.quietRollback();
            }
        }
    }
}
