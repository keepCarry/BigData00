package com.leopo.homework.work2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class MyCliet {

    public static void main(String[] args) {
        try{
            MyInterface proxy = RPC.getProxy(MyInterface.class, 1L, new InetSocketAddress("127.0.0.1", 12345), new Configuration());
            String res = proxy.getName("G20200388010284");
            System.out.println(res);
        } catch(IOException e){
            e.printStackTrace();
        }
    }

}
