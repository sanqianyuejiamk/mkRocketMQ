package com.mengka.mq.util;

import java.io.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * User: xiafeng
 * Date: 15-7-22
 */
public class ObjectUtil {

    private static Log log = LogFactory.getLog(ObjectUtil.class);

    public static Object changeByte2Object(byte[] bytes){
        Object object = null;
        try {
            ByteArrayInputStream byteInput = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInput = new ObjectInputStream(byteInput);
            object = objectInput.readObject();
            byteInput.close();
            objectInput.close();
        }catch (Exception e){
            log.error("changeByte2Object error!",e);
        }
        return object;
    }


    public static byte[] changeObject2byte(Object object){
        try {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            ObjectOutputStream objectOut = new ObjectOutputStream(
                    byteOut);
            objectOut.writeObject(object);

            objectOut.close();
            byteOut.close();
            return byteOut.toByteArray();
        }catch (Exception e){
            log.error("changeObject2byte error!",e);
        }
        return null;
    }
}
