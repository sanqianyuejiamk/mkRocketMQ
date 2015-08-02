package com.tongbanjie.rocketmq.monitor.client;

import com.tongbanjie.rocketmq.monitor.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ����ʵʱ��־
 *  <hr/>
 * -Dlog=/Users/hyy044101331/logs/rocketmqlogs/broker.log
 * -Dmode=client
 * -Dmport=5679
 * -Dserver=127.0.0.1
 * <p/>
 * Created by mengka
 */
public class Client {

    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private long lastTimeFileSize = 0;  //�ϴ��ļ���С

    private RandomAccessFile randomFile = null;


    public static void start() throws Exception {
        /**
         *  ��������
         */
        Socket socket = new Socket(Constant.DATA_IP, Constant.DATA_CLIENT_PORT);
        PrintWriter out = new PrintWriter(socket.getOutputStream());

        /**
         *  ����ʵʱ��־����
         */
        Client client = new Client();
        client.realtimeShowLog(out);

    }

    /**
     * @param out
     * @throws IOException
     */
    public void realtimeShowLog(final PrintWriter out) throws IOException {
        File logFile = new File(Constant.LOG_PATH);
        randomFile = new RandomAccessFile(logFile, "r");

        //����һ���߳�ÿ1���Ӷ�ȡ��������־��Ϣ
        ScheduledExecutorService executorService =
                Executors.newScheduledThreadPool(1);
        executorService.scheduleWithFixedDelay(new RealtimeLogTask(out), 0, 1, TimeUnit.SECONDS);
    }


    /**
     * ʵʱ��ȡ��־�ļ�task
     */
    public class RealtimeLogTask implements Runnable {

        private PrintWriter out;

        public RealtimeLogTask(PrintWriter out) {
            this.out = out;
        }

        public void run() {
            try {
                //��ñ仯���ֵ�
                randomFile.seek(lastTimeFileSize);

                String tmp = "";
                while ((tmp = randomFile.readLine()) != null) {
                    //log.info("lastTimeFileSize = "+lastTimeFileSize);
                    //log.info("----------------, realtimeLogTask run...");
//                    log.info(new String(tmp.getBytes("ISO8859-1")));
                    out.println(new String(tmp.getBytes("ISO8859-1")));
                    out.flush();
                }
                lastTimeFileSize = randomFile.length();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


}