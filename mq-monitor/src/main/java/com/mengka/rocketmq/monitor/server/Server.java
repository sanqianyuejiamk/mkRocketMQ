package com.mengka.rocketmq.monitor.server;

import com.mengka.rocketmq.monitor.constant.Constant;
import com.mengka.rocketmq.monitor.server.observer.MessageObserver;
import com.mengka.rocketmq.monitor.server.observer.QueueObserver;
import com.mengka.rocketmq.monitor.server.subject.RocketmqSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * ����client�����������־����
 * <hr/>
 * -Dlog=/Users/hyy044101331/logs/rocketmqlogs/broker.log
 * -Dmode=server
 * -Dmport=5679
 * <p/>
 * Created by mengka
 */
public class Server {

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private static Socket clientSocket;

    private static RocketmqSubject rocketmqSubject = RocketmqSubject.getInitializer();

    private static MessageObserver messageObserver = MessageObserver.getInitializer();

    private static QueueObserver queueObserver = new QueueObserver();

    public static void start() throws Exception {
        //��ӹ۲���
        rocketmqSubject.addObserver(messageObserver);
//        rocketmqSubject.addObserver(queueObserver);
        log.info("rocketmqSubject addObserver messageObserver!");

        /**
         *  ����log����
         */
        ServerSocket serverSocket_client = new ServerSocket(Constant.DATA_CLIENT_PORT);
        clientSocket = serverSocket_client.accept();
        invoke(clientSocket);
    }

    /**
     * ת����Ϣ��observer
     *
     * @param client
     * @throws Exception
     */
    private static void invoke(final Socket client) throws Exception {
        new Thread(new Runnable() {
            public void run() {
                BufferedReader clientReader = null;
                try {
                    clientReader = new BufferedReader(new InputStreamReader(client.getInputStream()));

                    while (true) {
                        /**
                         *  ��������
                         */
                        String msg = clientReader.readLine();

                        /**
                         * ����ת���͵����observer
                         */
                        rocketmqSubject.setChanged();
                        rocketmqSubject.notifyObservers(msg);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    //�ͷ���Դ
                    try {
                        clientReader.close();
                    } catch (Exception e) {
                    }
                    try {
                        clientSocket.close();
                    } catch (Exception e) {
                    }
                }
            }
        }).start();
    }
}
