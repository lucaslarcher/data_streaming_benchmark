package com.org.datastream.count_words;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class DataServer
{
    public static void main(String[] args) throws IOException
    {
        int port = 9999;
        String vec [] = "arroz feijao maça cacildes abacate choriço banana tenis carroceria medicamento jamanta".split(" ");
        ServerSocket listener = new ServerSocket(port);
        Random gerador = new Random();
        try{
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;
                while (true){
                    int r = gerador.nextInt();
                    if (r<0)
                        r *= -1;

                    String value = vec[r % vec.length];
                    out.println(value);
                    System.out.println(value);
                    Thread.sleep(50);
                }

            }
            catch (Exception e){
                System.out.println(e);
            }
            finally{
                socket.close();
            }

        } catch(Exception e ){
            System.out.println(e);
            e.printStackTrace();
        } finally{

            listener.close();
        }
    }
}
