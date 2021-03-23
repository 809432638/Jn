package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;

/**
 * 1. append used for appending contents to cell's value
 * 2. same as put when cell not exits
 */
public class AppendAndPut {
    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("ispace:itable");
        // create the test table ispace:itable with cf1, cf2
        // with max values 3
        new CreateTestTable().runCreate();

        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable(tableName)) {
            Put put = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1);
            table.put(put);
            System.out.println(StringUtils.rightPad("- test data: ", 120, "-"));
            util.getFullScanData(tableName);

            Append append = new Append(HBaseInfo.ROW1)
                    .add(HBaseInfo.CF1, HBaseInfo.F1C1, Bytes.toBytes("append"));
            table.append(append);
            Append append1 = new Append(HBaseInfo.ROW1)
                    .add(HBaseInfo.CF1, HBaseInfo.F1C2, Bytes.toBytes(
                            "append1"));
            table.append(append1);
            System.out.println(StringUtils.rightPad("\n- data after 2 appends: "
                    , 120, "-"));
            util.getFullScanData(tableName);
        }
    }
}
