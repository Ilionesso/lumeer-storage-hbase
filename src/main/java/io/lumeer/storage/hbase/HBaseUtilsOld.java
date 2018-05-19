package io.lumeer.storage.hbase;

import java.util.UUID;

/**
 * Created by Ilia Sheiko on 16/05/2018.
 */
public class HBaseUtilsOld {

    public static String generateUniqueId(String PID){
//        return InetAddress.getLocalHost().getHostName() + PID + new Timestamp(System.currentTimeMillis());
        return UUID.randomUUID().toString()+PID;
    }
}
