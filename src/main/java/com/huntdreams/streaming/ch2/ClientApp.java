package com.huntdreams.streaming.ch2;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * ClientApp
 *
 * @author tyee.noprom@qq.com
 * @time 2/21/16 1:47 PM.
 */
public class ClientApp {

    public static void main(String[] args) {
        try{
            System.out.println("Defining new Socket");
            ServerSocket serverSocket = new ServerSocket(9087);
            System.out.println("Waiting for incoming connection");

            Socket clientSocket = serverSocket.accept();
            System.out.println("Connection received");
            OutputStream outputStream = clientSocket.getOutputStream();
            // 持续监听端口并且发送数据给服务器端
            while (true) {
                PrintWriter out = new PrintWriter(outputStream, true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Waiting for user to input some data");
                String data = reader.readLine();
                System.out.println("Data received and now writing it to socket");
                out.println(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}