package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Table;
import util.HBaseUtil;

import java.io.IOException;

public class basictest {
    public static void main(String[] args) throws IOException {
        try(HBaseUtil util = HBaseUtil.getInstance(new Configuration());
                Table table = util.getTable("ispace:itable")) {
            //util.getTableInfo("ispace:itable");
            //util.getFullScanData("nst:testtable");

        }
    }
}
