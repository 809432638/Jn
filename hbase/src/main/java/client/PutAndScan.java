package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PutAndScan {
    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("ispace:itable");
        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable(tableName)) {
            // create test table ispace:itable with cf1, cf2 and max versions 3
            new CreateTestTable().runCreate();

            // test 1: single put and scan
            Put put1 = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2);
            table.put(put1);
            Scan scan1 = new Scan().setMaxVersions();
            ResultScanner scanner1 = table.getScanner(scan1);
            System.out.println(StringUtils.rightPad("- test 1 scan ", 120,
                    "-"));
            scanner1.forEach(util::getResultData);
            scanner1.close();

            // test 2 one put with multi cells( incorrect cell included )
            Put put2 = new Put(HBaseInfo.ROW2)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                    .addColumn(Bytes.toBytes("Bogus"), HBaseInfo.F1C2,
                            HBaseInfo.V2)
                    .addColumn(HBaseInfo.CF2, HBaseInfo.F2C1, HBaseInfo.V3);
            try {
                table.put(put2);
            } catch (Exception e) {
                System.out.println(StringUtils.rightPad("- test 2 begin ", 120,
                        "-"));
                System.out.println(e.getMessage());
            }
            ResultScanner scanner2 =
                    table.getScanner(new Scan().setMaxVersions());
            System.out.println(StringUtils.rightPad("- test 2 scan ", 120,
                    "-"));
            scanner2.forEach(util::getResultData);
            scanner2.close();

            // test 3 multi put (one incorrect put)
            List<Put> putList = new ArrayList<>();
            Put put31 = new Put(HBaseInfo.ROW3)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1);
            putList.add(put31);

            Put put32 = new Put(HBaseInfo.ROW3)
                    .addColumn(Bytes.toBytes("Bogus"), HBaseInfo.F1C2,
                            HBaseInfo.V2);
            putList.add(put32);

            Put put33 = new Put(HBaseInfo.ROW3)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V3);
            putList.add(put33);
            try {
                System.out.println(StringUtils.rightPad("- begin test 3 ", 120,
                        "-"));
                System.out.println("putlist size before action: " + putList.size());
                table.put(putList);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("putlist size after action: " + putList.size());
            }
            ResultScanner scanner3 =
                    table.getScanner(new Scan().setMaxVersions());
            System.out.println(StringUtils.rightPad("- test 3 scan ", 120,
                    "-"));
            scanner3.forEach(util::getResultData);
            scanner3.close();

            // test 4 bufferedMutator
            BufferedMutator mutator =
                    util.getConnection().getBufferedMutator(tableName);
            Put put41 = new Put(HBaseInfo.ROW4)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1);
            mutator.mutate(put41);
            Put put42 = new Put(HBaseInfo.ROW4)
                    .addColumn(Bytes.toBytes("Bogus"), HBaseInfo.F1C1,
                            HBaseInfo.V1);
            mutator.mutate(put42);
            Put put43 = new Put(HBaseInfo.ROW4)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V3);
            mutator.mutate(put43);

            try {
                System.out.println(StringUtils.rightPad("- begin to flush in " +
                                "test 4 ", 120,
                        "-"));
                mutator.flush();
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("mutator's WriteBufferSize: " +
                        mutator.getWriteBufferSize());
            } finally {
                mutator.close();
            }
            ResultScanner scanner4 =
                    table.getScanner(new Scan().setMaxVersions());
            System.out.println(StringUtils.rightPad("- test 4 scan ", 120,
                    "-"));
            scanner4.forEach(util::getResultData);
            scanner4.close();
        }
    }
}
