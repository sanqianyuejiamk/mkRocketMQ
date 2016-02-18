package com.tongbanjie.mq.common;

import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;


public class MqStoreComponent {

    private static final Logger logger = LoggerFactory.getLogger(MqStoreComponent.class);

    private H2Component h2Component = H2Component.getH2Component();

    private ReentrantLock lock = new ReentrantLock();

    private Connection connection;

    private Statement statement;

    private MqStoreComponent(){
    }

    /**
     *  查询message
     *
     * @return
     * @throws Exception
     */
    public List<MessageExt> getMessageExts()throws Exception{
        lock.lock();
        connection = h2Component.getConnection();
        statement = connection.createStatement();
        List<MessageExt> list = new ArrayList<MessageExt>();
        try{
            createTable(SQLConstant.MQ_TABLE_NAME,SQLConstant.MQ_ATTRIBUTE_ID,SQLConstant.MQ_ATTRIBUTE_CONTENT,SQLConstant.MQ_ATTRIBUTE_TAGS,SQLConstant.MQ_ATTRIBUTE_KEYS,SQLConstant.MQ_ATTRIBUTE_STATUS);

            String sql = "SELECT * FROM mq_message where status=0;";
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                MessageExt messageExt = new MessageExt();
                messageExt.setMsgId(rs.getString(SQLConstant.MQ_ATTRIBUTE_ID));
                messageExt.setBody(rs.getString(SQLConstant.MQ_ATTRIBUTE_CONTENT).getBytes());
                messageExt.setTags(rs.getString(SQLConstant.MQ_ATTRIBUTE_TAGS));
                messageExt.setKeys(rs.getString(SQLConstant.MQ_ATTRIBUTE_KEYS));
                list.add(messageExt);
            }
        }catch (Exception e){
            logger.error("getMessageExts error!",e);
        }finally {
            statement.close();
            connection.close();
            lock.unlock();
        }
        return list;
    }

    /**
     *  更新message状态
     *
     * @param id
     * @throws Exception
     */
    public void update(String id)throws Exception{
        lock.lock();
        connection = h2Component.getConnection();
        statement = connection.createStatement();
        try{
            String sql = "update mq_message set status=1 where id='"+id+"';";
            statement.executeUpdate(sql);
            logger.info("reconsumer message success! id = "+id);
        }catch (Exception e){
            logger.error("update error! id = "+id,e);
        }finally {
            statement.close();
            connection.close();
            lock.unlock();
        }
    }

    private MessageExt getMessageExtById(String id)throws Exception{
        try{
            String sql = "SELECT * FROM mq_message where id='"+id+"';";
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                MessageExt messageExt = new MessageExt();
                messageExt.setMsgId(rs.getString(SQLConstant.MQ_ATTRIBUTE_ID));
                messageExt.setBody(rs.getString(SQLConstant.MQ_ATTRIBUTE_CONTENT).getBytes());
                messageExt.setTags(rs.getString(SQLConstant.MQ_ATTRIBUTE_TAGS));
                messageExt.setKeys(rs.getString(SQLConstant.MQ_ATTRIBUTE_KEYS));
                return messageExt;
            }
        }catch (Exception e){
            logger.error("getMessageExts error!",e);
        }
        return null;
    }

    private void deleteById(String id)throws Exception{
        try{
            String sql = "delete FROM mq_message where id='"+id+"';";
            statement.executeUpdate(sql);
        }catch (Exception e){
            logger.error("getMessageExts error!",e);
        }
    }

    /**
     *  保存messageExt
     *
     * @param messageExt
     */
    public void saveMessageExt(MessageExt messageExt)throws Exception{
        lock.lock();
        connection = h2Component.getConnection();
        statement = connection.createStatement();
        try{
            createTable(SQLConstant.MQ_TABLE_NAME,SQLConstant.MQ_ATTRIBUTE_ID,SQLConstant.MQ_ATTRIBUTE_CONTENT,SQLConstant.MQ_ATTRIBUTE_TAGS,SQLConstant.MQ_ATTRIBUTE_KEYS,SQLConstant.MQ_ATTRIBUTE_STATUS);
            if(messageExt==null){
                logger.error("saveMessageExt messageExt is null!");
                return;
            }
            String sql= String.format(SQLConstant.sql_insert_table_message,SQLConstant.MQ_TABLE_NAME,messageExt.getMsgId(),new String(messageExt.getBody()),messageExt.getTags(),messageExt.getKeys(),0);
            MessageExt existMessageExt = getMessageExtById(messageExt.getMsgId());
            if(existMessageExt!=null){
                logger.info("delele message id = "+messageExt.getMsgId());
                deleteById(messageExt.getMsgId());
            }
            statement.executeUpdate(sql);
            logger.info("saveMessageExt messageExt success! id = "+messageExt.getMsgId());
        }catch (Exception e){
            logger.error("saveMessageExt error! id = "+messageExt.getMsgId(),e);
        }finally {
            statement.close();
            connection.close();
            lock.unlock();
        }
    }

    private void createTable(String tableName, String... attributes) {
        try {
            if(isExistsTable(tableName)){
                return;
            }
            if (attributes.length <= 0) {
                logger.error("attributes is empty!");
                return;
            }
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < attributes.length; i++) {
                if(i==attributes.length -1) {
                    buffer.append(attributes[i] + " VARCHAR(255)");
                }else{
                    buffer.append(attributes[i] + " VARCHAR(255),");
                }
            }
            String sql = String.format(SQLConstant.sql_create_table, tableName, buffer.toString());
            statement.executeUpdate(sql);
        } catch (Exception e) {
            logger.error("createTable error!", e);
        }
    }

    private boolean isExistsTable(String tableName){
        boolean isExistsTable = false;
        try{
            List<String> tables = showTables();
            if(tables.contains(tableName.toUpperCase())){
                isExistsTable = true;
            }
        }catch (Exception e){
            logger.error("isExistsTable error!",e);
        }
        return isExistsTable;
    }

    private List<String> showTables(){
        List<String> tables = new ArrayList<String>();
        try{
            ResultSet rs = statement.executeQuery(SQLConstant.sql_show_tables);
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }
        }catch (Exception e){
            logger.error("showTables error!",e);
        }
        return tables;
    }

    public static MqStoreComponent getMqStoreComponent(){
        return MqStoreComponentHolder.mqStoreComponent_Holder;
    }

    private static class MqStoreComponentHolder{
        private static MqStoreComponent mqStoreComponent_Holder = new MqStoreComponent();
    }

}
