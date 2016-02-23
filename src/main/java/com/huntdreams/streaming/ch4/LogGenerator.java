package com.huntdreams.streaming.ch4;

import java.io.*;

/**
 * LogGenerator
 *
 * @author tyee.noprom@qq.com
 * @time 2/23/16 11:02 AM.
 */
public class LogGenerator {

    public static void main(String[] args) {

        try {
            if (args.length != 2) {
                System.out.println("Usage - java LogGenerator <Location\n" +
                        " of Log File to be read> <location of the log file in which logs needs\n" +
                        " to be updated>");
                System.exit(0);
            }

            String location = args[0];
            File f = new File(location);
            FileOutputStream writer = new FileOutputStream(f);

            File read = new File(args[1]);
            BufferedReader reader = new BufferedReader(new FileReader(read));

            for (;;) {
                writer.write((reader.readLine()+"\n").getBytes());
                writer.flush();
                Thread.sleep(500);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
