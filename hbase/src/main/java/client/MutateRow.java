package client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;

/**
 * RowMutations.add(put/delete), multiple actions for the same row
 * , actions keep the order of adding into list
 */
public class MutateRow {
    public static void main(String[] args) throws IOException {
        // initial table ispace:itable with cf1,cf2 and max versions 3
        new CreateTestTable().runCreate();

        TableName tableName = TableName.valueOf("ispace:itable");
        try(HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable(tableName)) {
            RowMutations rowMutations = new RowMutations(HBaseInfo.ROW1);
            Put put1 = new Put(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2)
                    .addColumn(HBaseInfo.CF2, HBaseInfo.F2C1, HBaseInfo.V3);

            Delete delete = new Delete(HBaseInfo.ROW1)
                    .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2);

            rowMutations.add(put1);
            rowMutations.add(delete);

            table.mutateRow(rowMutations);
            util.getFullScanData(tableName);
        }
    }
}
