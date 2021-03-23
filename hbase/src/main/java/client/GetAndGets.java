package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetAndGets {
    public static void main(String[] args) throws IOException {
        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                Table table = util.getTable("ispace:itable")) {
            // single get
            Get get =
                    new Get(Bytes.toBytes("row1")).addColumn(Bytes.toBytes(
                            "cf1"), Bytes.toBytes(
                            "f1c1")).setMaxVersions();
            Result result = table.get(get);
            util.getResultData(result);
            System.out.println(StringUtils.rightPad("-", 120, "-"));
            // get list
            List<Get> gets = new ArrayList<>();
            Get get1 =
                    new Get(Bytes.toBytes("row1")).addFamily(Bytes.toBytes(
                            "cf1")).setMaxVersions(3);
            gets.add(get1);
            Get get2 = new Get(Bytes.toBytes("row2"));
            gets.add(get2);

            Result[] results = table.get(gets);
            Arrays.asList(results).forEach(util::getResultData);
        }
    }
}
