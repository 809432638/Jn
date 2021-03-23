package client;

import org.apache.hadoop.hbase.client.Put;
import util.HBaseInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Fingerprint {
    public static void main(String[] args) throws IOException {
        Put put = new Put(HBaseInfo.ROW1)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V3)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C4, HBaseInfo.V4)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C5, HBaseInfo.V5);

        String id = String.format("Hostname: %s, App: %s",
                InetAddress.getLocalHost().getHostName(),
                System.getProperty("sun.java.command"));
        put.setId(id);

        System.out.println("put.size: " + put.size());
        System.out.println("put.id: " + put.getId());
        System.out.println("put.fingerprint: " + put.getFingerprint());
        System.out.println("put.toMap: " + put.toMap(6));
        System.out.println("put.toJSON: " + put.toJSON(6));
        System.out.println("put.toString: " + put.toString(6));
    }
}
