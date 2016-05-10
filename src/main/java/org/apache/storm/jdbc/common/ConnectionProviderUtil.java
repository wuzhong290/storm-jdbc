package org.apache.storm.jdbc.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by wuzhong on 2016/5/10.
 */
public class ConnectionProviderUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProviderUtil.class);

    private static ConnectionProvider connectionProvider;

    public synchronized static ConnectionProvider getConnectionProvider(Map hikariConfigMap){
        if(null == connectionProvider){
            LOGGER.info("new connectionProvider:"+Thread.currentThread().getName());
            connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        }else{
            LOGGER.info("old connectionProvider:"+Thread.currentThread().getName());
        }
        return connectionProvider;
    }
}
