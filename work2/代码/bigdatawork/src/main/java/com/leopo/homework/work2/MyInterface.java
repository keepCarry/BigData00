package com.leopo.homework.work2;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInterface extends VersionedProtocol{
    long versionID = 1L;
    String getName(String code);
}
