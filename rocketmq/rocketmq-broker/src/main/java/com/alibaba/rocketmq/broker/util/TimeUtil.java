package com.alibaba.rocketmq.broker.util;


import org.apache.commons.lang.StringUtils;
import java.text.SimpleDateFormat;

/**
 * User: mengka
 * Date: 14-5-26
 * Time: 上午10:47
 */
public class TimeUtil {

    public static final String format_1 = "yyyy-MM-dd HH:mm:ss";

    public static final String format_2 = "yyyyMMddHHmmss";

    public static final String format_3 = "yyyy-MM-dd";

    public static final String format_4 = "yyyy年MM月dd日";


    /**
     * 将String类型的日期转成Date类型
     *
     * @param dt
     *            date object
     * @param sFmt
     *            the date format
     *
     * @return the formatted string
     */
    public static String toDate(java.util.Date dt, String sFmt) {
        if (null == dt || StringUtils.isBlank(sFmt)) {
            return null;
        }
        SimpleDateFormat sdfFrom = null;
        String sRet = null;
        try {
            sdfFrom = new SimpleDateFormat(sFmt);
            sRet = sdfFrom.format(dt).toString();
        } catch (Exception ex) {
            return null;
        } finally {
            sdfFrom = null;
        }
        return sRet;
    }

    /**
     *  将"yyyy-MM-dd HH:mm:ss"格式的string转变成日期
     *  <hr>
     *  日期格式：
     *  <ul>
     *     <li>"yyyy-MM-dd HH:mm:ss"</li>
     *     <li>"yyyy-MM-dd"</li>
     *  </ul>
     *
     * @param sDate
     * @param sFmt 日期字符串,"yyyy-MM-dd HH:mm:ss"
     * @return
     */
    public static java.util.Date toDate(String sDate, String sFmt) {
        if (StringUtils.isBlank(sDate) || StringUtils.isBlank(sFmt)) {
            return null;
        }

        SimpleDateFormat sdfFrom = null;
        java.util.Date dt = null;
        try {
            sdfFrom = new SimpleDateFormat(sFmt);
            dt = sdfFrom.parse(sDate);
        } catch (Exception ex) {
            return null;
        } finally {
            sdfFrom = null;
        }

        return dt;
    }
}