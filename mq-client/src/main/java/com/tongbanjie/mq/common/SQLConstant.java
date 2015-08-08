package com.tongbanjie.mq.common;


public class SQLConstant {

    public static final String sql_show_tables = "show tables;";

    public static final String sql_create_table = "CREATE TABLE %s(%s);";

    public static final String sql_insert_table_message = "INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s');";

    /**
     * message attribute
     */
    public static final String MQ_TABLE_NAME = "mq_message";

    public static final String MQ_ATTRIBUTE_ID = "id";
    public static final String MQ_ATTRIBUTE_CONTENT = "content";
    public static final String MQ_ATTRIBUTE_TAGS = "tags";
    public static final String MQ_ATTRIBUTE_KEYS = "keys";
    public static final String MQ_ATTRIBUTE_STATUS = "status";

}
