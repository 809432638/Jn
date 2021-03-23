package client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import util.HBaseUtil;

import java.io.IOException;

public class CreateTestTable {
    public void runCreate() throws IOException {
        try (HBaseUtil util = HBaseUtil.getInstance(HBaseConfiguration.create())) {
            String spaceName = "ispace";
            String tableName = "itable";

            util.dropNamespace(spaceName, true);
            util.createNamespace("ispace");

            util.createTable(TableName.valueOf(spaceName + ":" + tableName), 3,
                    "cf1", "cf2");
        }
    }

    public static void main(String[] args) throws IOException {
        new CreateTestTable().runCreate();
    }
}
