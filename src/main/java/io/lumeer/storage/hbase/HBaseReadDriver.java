package io.lumeer.storage.hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Ilia Sheiko on 05/04/2018.
 */
public class HBaseReadDriver {

    public static void main(String[] args){
        HBaseReadDriver hBaseReadDriver = new HBaseReadDriver();
        hBaseReadDriver.run();
    }

    private void run(){
        Configuration config = HBaseConfiguration.create();

        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        try {
            Connection conn = ConnectionFactory.createConnection(config);
            HBaseAdmin.checkHBaseAvailable(config);
            Admin admin = conn.getAdmin();

            HTable hTable = new HTable(config, "emp");
            Put p = new Put(Bytes.toBytes("testEmp1"));
            p.add(Bytes.toBytes("personal data"), Bytes.toBytes("age"),Bytes.toBytes("5"));
            p.add(Bytes.toBytes("professional data"), Bytes.toBytes("job"),Bytes.toBytes("cookie_destroyer"));
            p.add(Bytes.toBytes("professional data"), Bytes.toBytes("alignment"),Bytes.toBytes("lawful_evil"));
            hTable.put(p);
            Get get = new Get(Bytes.toBytes("testEmp1"));
            Result result = hTable.get(get);
            byte [] value = result.getValue(Bytes.toBytes("personal data"),Bytes.toBytes("age"));
            String age = Bytes.toString(value);
            System.out.println("age: " + age);
            hTable.close();

        } catch (ServiceException | IOException e) {
            e.printStackTrace();
        }
    }
}

