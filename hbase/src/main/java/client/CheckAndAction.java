package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * table.checkAndAction:<br/>
 *  checkAndGet<br/>
 *  checkAndPut<br/>
 *  checkAndDelete<br/>
 *  checkAndMutate<br/>
 * <br/>
 *  使用比较符判断，注意是传入的值与单元格内的值比较
 */
public class CheckAndAction {
    public static void main(String[] args) throws IOException,
            InterruptedException {
        TableName tableName = TableName.valueOf("ispace:itable");
        // create test table ispace:itable with cf1,cf2 and max versions 3
        new CreateTestTable().runCreate();

        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable(tableName)) {
            List<Row> rows = new ArrayList<>();
            Put put1 = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2);
            rows.add(put1);
            Put put2 = new Put(HBaseInfo.ROW2)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V3)
                    .addColumn(HBaseInfo.CF2, HBaseInfo.F2C1, HBaseInfo.V4);
            rows.add(put2);
            Result[] results = new Result[rows.size()];
            table.batch(rows, results);
            System.out.println(StringUtils.rightPad("- initial data", 120,
                    "-"));
            util.getFullScanData(tableName);

            // test 1: 检查的是 row1:cf1:f1c2,值为null表示不存在，实际存在，所以不删除
            Delete delete1 = new Delete(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1);
            table.checkAndDelete(HBaseInfo.ROW1, HBaseInfo.CF1,
                    HBaseInfo.F1C2, null, delete1);
            System.out.println(StringUtils.rightPad("- test1 ", 120,
                    "-"));
            util.getFullScanData(tableName);

            // test 2: 手工删除 row1:cf1:f1c2, 这次删除成功
            table.delete(new Delete(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2));
            table.checkAndDelete(HBaseInfo.ROW1, HBaseInfo.CF1,
                    HBaseInfo.F1C2, null, delete1);
            System.out.println(StringUtils.rightPad("- test2 ", 120,
                    "-"));
            util.getFullScanData(tableName);

            // test 3: 判断的是 row2,删除的是 row1, 报错
            try {
                table.checkAndDelete(HBaseInfo.ROW2, HBaseInfo.CF1,
                        HBaseInfo.F1C1, CompareFilter.CompareOp.NOT_EQUAL,
                        HBaseInfo.V5,
                        new Delete(HBaseInfo.ROW1).addFamily(HBaseInfo.CF1));
            } catch (Exception e) {
                // Action's getRow must match the passed row
                System.out.println("error: " + e.getMessage());
            }
            System.out.println(StringUtils.rightPad("- test3 ", 120,
                    "-"));
            util.getFullScanData(tableName);
        }
    }
}
