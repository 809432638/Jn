package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import util.HBaseUtil;

import java.io.IOException;
import java.util.Arrays;

public class CreateTable {
    public void executeCreate(TableName tableName, byte[][] splitKeys,
            int maxVersions, String... families) throws IOException {
        try(Connection connection =
                ConnectionFactory.createConnection(HBaseConfiguration.create());
                Admin admin = connection.getAdmin()) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

            Arrays.asList(families).forEach(x -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(x);
                columnDescriptor.setMaxVersions(StringUtils.isEmpty(String.valueOf(maxVersions)) ? 1 : maxVersions);
                tableDescriptor.addFamily(columnDescriptor);
            });

            if(splitKeys != null) {
                admin.createTable(tableDescriptor, splitKeys);
            } else {
                admin.createTable(tableDescriptor);
            }

            System.out.println(StringUtils.rightPad("-", 120, "-"));
            System.out.println(admin.getTableDescriptor(tableName).toString());
            System.out.println(StringUtils.rightPad("-", 120, "-"));
        }
    }

    public static void main(String[] args) throws IOException {

        TableName tableName = TableName.valueOf("ispace:itable");
        byte[][] splitKeys = null;
        String[] families = new String[]{"cf1", "cf2"};

        try(HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());) {
            util.dropNamespace("ispace", true); // delete namespace
            util.createNamespace("ispace"); // rebuild namespace
        }

        new CreateTable().executeCreate(tableName, splitKeys, 3, families);
    }
}
