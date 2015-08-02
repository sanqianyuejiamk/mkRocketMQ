package com.tongbanjie.rocketmq.monitor.server.util;

import org.apache.commons.lang.StringUtils;
import java.text.SimpleDateFormat;
/**
 * Created by xiafeng
 * on 2015/5/20.
 */
public class TimeUtil {

    public static final String format_1 = "yyyy-MM-dd HH:mm:ss";

    public static final String format_2 = "yyyyMMddHHmmss";

    public static final String format_3 = "yyyy-MM-dd";

    public static final String format_4 = "yyyy��MM��dd��";

    public static final String format_5 = "yyyyMMdd";

    public static final String format_6 = "yyyyMM";

    /**
     * ��String���͵�����ת��Date����
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
     *  ��"yyyy-MM-dd HH:mm:ss"��ʽ��stringת�������
     *  <hr>
     *  ���ڸ�ʽ��
     *  <ul>
     *     <li>"yyyy-MM-dd HH:mm:ss"</li>
     *     <li>"yyyy-MM-dd"</li>
     *  </ul>
     *
     * @param sDate
     * @param sFmt �����ַ���,"yyyy-MM-dd HH:mm:ss"
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