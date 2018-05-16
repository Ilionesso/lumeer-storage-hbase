package io.lumeer.storage.hbase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;

/**
 * Created by Ilia Sheiko on 16/05/2018.
 */
public class HBaseUtilsOld {

    public static String generateUniqueId(String PID){
        try {
            return InetAddress.getLocalHost().getHostName() + PID + new Timestamp(System.currentTimeMillis());
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return PID + new Timestamp(System.currentTimeMillis());
        }
    }
}
