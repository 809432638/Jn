package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;

public class GetRowOffset {
    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("ispace:itable");
        // create a new table ispace:itable with cf1,cf2 and max version=3
        new CreateTestTable().runCreate();
        try(HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable(tableName)){
            Put put1 = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V3)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C4, HBaseInfo.V4)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C5, HBaseInfo.V5)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C6, HBaseInfo.V6)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C7, HBaseInfo.V7);
            table.put(put1);
            Put put2 = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C5, HBaseInfo.V9);
            table.put(put2);
            System.out.println(StringUtils.rightPad("- initial data ", 120,
                    "-"));
            util.getFullScanData(tableName);
            System.out.println(StringUtils.rightPad("- begin test ", 120,
                    "-"));
            Get get = new Get(HBaseInfo.ROW1)
                    .setRowOffsetPerColumnFamily(3)
                    .setMaxResultsPerColumnFamily(4)
                    .setMaxVersions(3);
            util.getResultData(table.get(get));
        }
    }
}
