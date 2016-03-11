package com.mengka.mq.common;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;


public class H2Component {

    private static final Logger log = LoggerFactory.getLogger(H2Component.class);

    private static final String JDBC_URL="jdbc:h2:tcp://localhost/~/h2db";

    private static final String USER="sa";

    private static final String PASSWORD="";

    /**
     *  连接超时时间
     */
    private static final Integer H2_LOGIN_TIMEOUT = 10000;

    /**
     *  连接最大个数
     */
    private static final Integer H2_MAX_CONNECTIONS = 100;

    private Server server;

    /**
     * H2数据库自带的连接池
     */
    private static JdbcConnectionPool connectionPool = null;


    private H2Component(){
        start();
        initConnectionPool();
    }

    /**
     *  初始化连接池
     *
     */
    public void initConnectionPool(){
        try {
            connectionPool = JdbcConnectionPool.create(JDBC_URL, USER, PASSWORD);
            connectionPool.setLoginTimeout(H2_LOGIN_TIMEOUT);
            connectionPool.setMaxConnections(H2_MAX_CONNECTIONS);
        }catch (Exception e){
            log.error("init H2 JdbcConnectionPool error!",e);
        }
    }

    public Connection getConnection() throws Exception{
        return connectionPool.getConnection();
    }

    /**
     *  启动H2数据库
     *
     */
    public void start(){
        try {
            server = Server.createTcpServer().start();
            log.info("h2 DB start..");
        }catch (Exception e){
            log.error("h2 DB start error!",e);
        }
    }

    public void exit(){
        if(this.server!=null){
            this.server.stop();
            this.server = null;
        }
    }

    public static H2Component getH2Component(){
        return H2ComponentHolder.h2Component_holder;
    }

    private static class H2ComponentHolder{
        private static H2Component h2Component_holder = new H2Component();
    }
}
