package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * error handler
 * 1. insert a non-exists column family
 *      can catch the detail error information
 *      other put can be flushed into hbase
 * 2. insert a row with no contents
 *      all puts would not be executed, nothing inserted
 */
public class ErrorHandler {
    public static void main(String[] args) throws IOException {

        byte[] bogus = Bytes.toBytes("bogus");
        byte[] row1 = Bytes.toBytes("row1");
        byte[] row2 = Bytes.toBytes("row2");
        byte[] cf1 = Bytes.toBytes("cf1");
        byte[] cf2 = Bytes.toBytes("cf2");
        byte[] f1c1 = Bytes.toBytes("f1c1");
        byte[] f1c2 = Bytes.toBytes("f1c2");
        byte[] v1 = Bytes.toBytes("v1");
        byte[] v2 = Bytes.toBytes("v2");
        byte[] v3 = Bytes.toBytes("v3");
        byte[] v4 = Bytes.toBytes("v4");

        // initial the table ispace:itable with cf1,cf2 and max versions 3
        new CreateTestTable().runCreate();

        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable("ispace:itable")) {
            List<Put> puts = new ArrayList<>();
            Put put1 = new Put(row1).addColumn(cf1, f1c1, 1L, v1);
            puts.add(put1);
            // add a non-exists family 'bogus'
            Put put2 = new Put(row1).addColumn(bogus, f1c1, 2L, v2);
            puts.add(put2);
            Put put3 = new Put(row1).addColumn(cf1, f1c2, 1L, v3);
            puts.add(put3);

            System.out.println(StringUtils.rightPad("-", 100, "-"));
            System.out.println(" begin test 1");
            System.out.println(StringUtils.rightPad("-", 100, "-"));
            puts.forEach(System.out::println);

            try {
                table.put(puts);
            } catch (RetriesExhaustedWithDetailsException e) {
                // handle failure
                int numErrors = e.getNumExceptions();
                System.out.println(StringUtils.rightPad("-", 100, "-"));
                System.out.println("Number of Exceptions: " + numErrors);
                for (int i = 0; i < numErrors; i++) {
                    System.out.println("Cause[" + i + "]");
                    System.out.println("Hostname["+i+"]" + e.getHostnamePort(i));
                    System.out.println("Row["+i+"]"+e.getRow(i));
                }
                System.out.println(StringUtils.rightPad("-", 100, "-"));
                System.out.println("Cluster Issues: " + e.mayHaveClusterIssues());
                System.out.println("Description: " + e.getExhaustiveDescription());
                System.out.println(StringUtils.rightPad("-", 100, "-"));
            } catch (Exception e) {
                System.out.print("Error in test1 -> " + e);
            }
            util.getFullScanData(table.getName());
            System.out.println(StringUtils.rightPad("-", 100, "-"));
            System.out.println(" begin test 2");
            System.out.println(StringUtils.rightPad("-", 100, "-"));

            // initial the table ispace:itable with cf1,cf2 and max versions 3
            new CreateTestTable().runCreate();
            List<Put> puts2 = new ArrayList<>();
            puts.forEach(x -> puts2.add(x));
            // add a none-content row
            Put put4 = new Put(row2);
            puts2.add(put4);
            puts2.forEach(System.out::println);
            System.out.println(StringUtils.rightPad("-", 100, "-"));
            try{
                table.put(puts2);
            } catch (Exception e) {
                System.out.println("error in test2 -> " + e.getMessage());
            }

            System.out.println(StringUtils.rightPad("-", 100, "-"));
            util.getFullScanData(table.getName());
        }
    }
}
