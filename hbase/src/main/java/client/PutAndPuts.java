package client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutAndPuts {
    public void putAction(HBaseUtil util) throws IOException {
        Table table = util.getTable(TableName.valueOf("ispace:itable"));
        Put put = new Put(Bytes.toBytes("row1")).addColumn(Bytes.toBytes("cf1"),
                Bytes.toBytes("f1c1"), 1L, Bytes.toBytes("vf1c1"));
        table.put(put);

        util.getFullScanData(TableName.valueOf("ispace:itable"));
    }

    public void putsAction(HBaseUtil util) throws IOException {
        Table table = util.getTable(TableName.valueOf("ispace:itable"));
        List<Put> puts = new ArrayList<Put>();

        Put put1 = new Put(Bytes.toBytes("row1")).addColumn(Bytes.toBytes(
                "cf1"), Bytes.toBytes("f1c1"), 2L, Bytes.toBytes("vf1c0"));
        Put put2 = new Put(Bytes.toBytes("row1")).addColumn(Bytes.toBytes(
                "cf1"), Bytes.toBytes("f1c2"), 1L, Bytes.toBytes("vf1c2"));
        Put put3 = new Put(Bytes.toBytes("row1")).addColumn(Bytes.toBytes(
                "cf2"), Bytes.toBytes("f2c1"), 1L, Bytes.toBytes("vf2c1"));
        Put put4 = new Put(Bytes.toBytes("row2")).addColumn(Bytes.toBytes(
                "cf1"), Bytes.toBytes("f1c1"), 1L, Bytes.toBytes("vf1c1"));

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);

        util.getFullScanData(TableName.valueOf("ispace:itable"));
    }

    public static void main(String[] args) throws IOException {
        // initial test table ispace:itable with cf1,cf2 and maxVersion=3
        new CreateTestTable().runCreate();

        try(HBaseUtil util = HBaseUtil.getInstance(HBaseConfiguration.create())) {
            PutAndPuts instance = new PutAndPuts();
            System.out.println("--- put action -----------------");
            instance.putAction(util);
            System.out.println("--- puts action ----------------");
            instance.putsAction(util);
        }
    }
}
