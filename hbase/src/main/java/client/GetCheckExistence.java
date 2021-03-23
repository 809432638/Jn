package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetCheckExistence {
    /**
     * setCheckExistenceOnly 方法只检查是否存在并返回 bool
     * 如果get设置查询多个单元格，只要有一个存在即返回 true
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        TableName tableName = TableName.valueOf("ispace:itable");
        // create test table ispace:itable with cf1,cf2 and max versions: 3
        new CreateTestTable().runCreate();

        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable(tableName)) {
            List<Put> puts = new ArrayList<>();
            Put put1 = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1);
            puts.add(put1);
            Put put2 = new Put(HBaseInfo.ROW2)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V2)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V3);
            puts.add(put2);
            table.put(puts);
            System.out.println(StringUtils.rightPad("-- init data ", 120, "-"));
            util.getFullScanData(tableName);

            Get get1 = new Get(HBaseInfo.ROW2)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1)
                    .setCheckExistenceOnly(true);
            Result result1 = table.get(get1);

            System.out.println("\nresult1 exists: " + result1.getExists());
            System.out.println("result1 size: " + result1.size());
            System.out.println("result1 is empty: " + result1.isEmpty());

            Get get2 = new Get(HBaseInfo.ROW2)
                    .addColumn(HBaseInfo.CF1, Bytes.toBytes("qual-99"))
                    .setCheckExistenceOnly(true);
            Result result2 = table.get(get2);

            System.out.println("\nresult2 exists: " + result2.getExists());
            System.out.println("result2 size: " + result2.size());
            System.out.println("result2 is empty: " + result2.isEmpty());

        }
    }
}
