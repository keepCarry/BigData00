package com.leopo.homework.work2;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

class MyInterfaceImpl implements MyInterface {

    @Override
    public String getName(String code){
        String name = code.equals("G20200388010284") ? "liupeng" : null;
        System.out.println("the code " + code + "'s name is" + name);
        return name;
    }

    @Override
    public long getProtocolVersion(String proticol, long clientVersion){
        return MyInterface.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
