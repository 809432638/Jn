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

/**
 * batch action used for actions on multiple row<br/>
 * batch action executed by table.batch<br/>
 * batch method fit for Deletes, Gets, Puts, Increments and Appends.<br/>
 * The ordering of execution of the actions is not defined.
 */
public class BatchAction {
    public static void main(String[] args) throws IOException,
            InterruptedException {
        // initial the table ispace:itable with cf1,cf2 and max versions: 3
        new CreateTestTable().runCreate();

        TableName tableName = TableName.valueOf("ispace:itable");
        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable(tableName)) {
            // initial data
            Put put = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2);
            table.put(put);
            System.out.println(StringUtils.rightPad("- initial data: ", 120,
                    "-"));
            util.getFullScanData(tableName);
            System.out.println(StringUtils.rightPad("- begin batch test: ",
                    120, "-"));

            // begin batch test
            List<Row> batch = new ArrayList<>();
            Put put1 = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V3);
            batch.add(put1);

            Get get1 = new Get(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2);
            batch.add(get1);

            Delete delete1 = new Delete(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2);
            batch.add(delete1);

            Get get2 = new Get(HBaseInfo.ROW2)
                    .addColumn(HBaseInfo.CF2, Bytes.toBytes("Bogus"));
            batch.add(get2);

            Object[] results = new Object[batch.size()];
            // 1: normal batch
            // ????????? put/get/delete list
            // puts ??????????????????????????? ??????????????????????????????????????????????????????????????????????????????
            // gets ??????????????????????????????
            // batch(batch, results) ?????????????????????????????????????????????????????????????????????????????????????????????
            table.batch(batch, results);

            // 2: batch callback
            /*
            try {
                // batch with callback is same as batch(List, Object[]),
                // but with a callback
                // batchCallback ??? batch ?????????????????????????????????????????? callback
                // ???????????? final Batch.Callback<R> callback ??????
                // ????????? void update(byte[] region, byte[] row, R result);
                table.batchCallback(batch, results, (region, row, result) -> {
                    System.out.println("Received result for region[" + region
                            + "],row[" + row + "] ->" + result);
                });
            } catch (Exception e) {
                System.out.println("Exception in batchCallBack: "
                        + e.getMessage());
            }
            */

            // sleep 1sec to make sure finishing print the exception
            Thread.sleep(1000L);

            for(int i=0; i < results.length; i++){
                    System.out.println("Result[" + i + "] -> type: "
                            + results[i].getClass().getTypeName() + ", "
                            + results[i]);
            }

            util.getFullScanData(tableName);
        }
    }
}
