package com.tongbanjie.rocketmq.monitor.server;

import org.apache.log4j.Logger;

import com.tongbanjie.rocketmq.monitor.constant.Constant;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * ����client�����������־����
 * <p/>
 * Created by mengka
 */
public class Server {

    private static final Logger log = Logger.getLogger(Server.class);

    private static Socket clientSocket;

    private static ArrayList<PrintWriter> outs = new ArrayList<PrintWriter>();

    public static void main(String[] args) throws Exception {

        /**
         *  ����log����
         */
        ServerSocket serverSocket_client = new ServerSocket(Constant.DATA_CLIENT_PORT);
        clientSocket = serverSocket_client.accept();
        invoke(clientSocket, outs);


        ServerSocket serverSocket = new ServerSocket(Constant.DATA_PORT);
        while (true) {
            Socket socket = serverSocket.accept();
            PrintWriter pwriter = new PrintWriter(socket.getOutputStream());
            outs.add(pwriter);

            log.info("-----------, spout socket connected: "+socket.getInetAddress());

        }
    }

    /**
     *  ת����Ϣ��storm��spout
     *
     * @param client
     * @param writers
     * @throws Exception
     */
    private static void invoke(final Socket client, final ArrayList<PrintWriter> writers) throws Exception {
        new Thread(new Runnable() {
            public void run() {
                BufferedReader clientReader = null;
                PrintWriter clientWriter = null;
                PrintWriter writer = null;
                try {
                    clientReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    clientWriter = new PrintWriter(client.getOutputStream());

                    while (true) {
                        /**
                         *  ��������
                         */
                        String msg = clientReader.readLine();
                        clientWriter.println("Server received " + msg);
                        clientWriter.flush();

                        /**
                         * ����ת���͵����client
                         */
                        for (int i = 0; i < writers.size(); i++) {
                            writer = writers.get(i);
                            System.out.println("writer["+i+"] send msg: " + msg);
                            writer.println(msg);
                            writer.flush();
                        }

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
                        clientWriter.close();
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
