package com.mengka.rocketmq.monitor.boot;

import com.mengka.rocketmq.monitor.client.Client;
import com.mengka.rocketmq.monitor.server.Server;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: mengka
 * Date: 15-6-19-ÏÂÎç10:42
 */
public class Boot {

    private static Logger logger = LoggerFactory.getLogger(Boot.class);

    public static void main(String[] args){
        String mode = System.getProperty("mode");
        logger.info("--------------, run boot mode = "+mode+" , server = "+System.getProperty("server")+" , mport = "+System.getProperty("mport")+" , log = "+System.getProperty("log"));

        if(StringUtils.isBlank(mode)){
            logger.error("mode is blank!");
            return;
        }
        if("server".equals(mode)){
            startServer();
        }else if("client".equals(mode)){
            startClient();
        }
    }


    public static void startServer(){
        logger.info("mq-monitor startServer..");
        try {
            Server server = new Server();
            server.start();
        }catch (Exception e){
            logger.error("startServer error!",e);
        }
    }


    public static void startClient(){
        logger.info("mq-monitor startClient..");
        try{
            Client client = new Client();
            client.start();
        }catch (Exception e){
            logger.error("startClient error!",e);
        }
    }
}
