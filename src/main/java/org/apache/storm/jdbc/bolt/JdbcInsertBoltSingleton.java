package org.apache.storm.jdbc.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.ConnectionProviderUtil;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wuzhong on 2016/5/10.
 */
public class JdbcInsertBoltSingleton extends BaseRichBolt {
    protected OutputCollector collector;
    protected transient JdbcClient jdbcClient;
    protected Integer queryTimeoutSecs;
    protected ConnectionProvider connectionProvider;
    private String tableName;
    private String insertQuery;
    private JdbcMapper jdbcMapper;
    private String mapperTableName;
    private Map hikariConfigMap;

    public JdbcInsertBoltSingleton() {
    }
    //在构造方法中传入的参数在storm中就是每一个线程中就是一个实例，因为storm中每一个线程中对应一个spout、bolt实例，不同的线程对应的spout、bolt实例不同。
    public JdbcInsertBoltSingleton(ConnectionProvider connectionProvider) {
        Validate.notNull(connectionProvider);
        this.connectionProvider = connectionProvider;
    }

    public JdbcInsertBoltSingleton(ConnectionProvider connectionProvider, JdbcMapper jdbcMapper) {
        Validate.notNull(jdbcMapper);
        Validate.notNull(connectionProvider);
        this.jdbcMapper = jdbcMapper;
        this.connectionProvider = connectionProvider;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        if(StringUtils.isBlank(this.tableName) && StringUtils.isBlank(this.insertQuery)) {
            throw new IllegalArgumentException("You must supply either a tableName or an insert Query.");
        }
        if(StringUtils.isBlank(this.mapperTableName) && null ==this.jdbcMapper) {
            throw new IllegalArgumentException("You must supply either a mapperTableName or an jdbcMapper.");
        }
        if(null == hikariConfigMap && null ==this.connectionProvider) {
            throw new IllegalArgumentException("You must supply either a hikariConfigMap or an connectionProvider.");
        }
        if(null ==this.connectionProvider){
            //通过这种方式JdbcBoltFactory.getConnectionProvider()可以实现一个storm进程中对应一个实例connectionProvider
            connectionProvider = ConnectionProviderUtil.getConnectionProvider(hikariConfigMap);
        }
        if(null ==this.jdbcMapper){
            jdbcMapper = new SimpleJdbcMapper(mapperTableName, connectionProvider);
        }

        this.collector = collector;
        this.connectionProvider.prepare();
        if(this.queryTimeoutSecs == null) {
            this.queryTimeoutSecs = Integer.valueOf(Integer.parseInt(map.get("topology.message.timeout.secs").toString()));
        }

        this.jdbcClient = new JdbcClient(this.connectionProvider, this.queryTimeoutSecs.intValue());
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            List e = this.jdbcMapper.getColumns(tuple);
            ArrayList columnLists = new ArrayList();
            columnLists.add(e);
            if(!StringUtils.isBlank(this.tableName)) {
                this.jdbcClient.insert(this.tableName, columnLists);
            } else {
                this.jdbcClient.executeInsertQuery(this.insertQuery, columnLists);
            }

            this.collector.ack(tuple);
        } catch (Exception var4) {
            this.collector.reportError(var4);
            this.collector.fail(tuple);
        }
    }


    public void cleanup() {
        this.connectionProvider.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public JdbcInsertBoltSingleton withTableName(String tableName) {
        if(this.insertQuery != null) {
            throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
        } else {
            this.tableName = tableName;
            return this;
        }
    }

    public JdbcInsertBoltSingleton withInsertQuery(String insertQuery) {
        if(this.tableName != null) {
            throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
        } else {
            this.insertQuery = insertQuery;
            return this;
        }
    }

    public JdbcInsertBoltSingleton withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = Integer.valueOf(queryTimeoutSecs);
        return this;
    }

    public JdbcInsertBoltSingleton withMapperTableName(String mapperTableName) {
        this.mapperTableName = mapperTableName;
        return this;
    }

    public JdbcInsertBoltSingleton withHikariConfigMap(Map hikariConfigMap) {
        this.hikariConfigMap = hikariConfigMap;
        return this;
    }
}
