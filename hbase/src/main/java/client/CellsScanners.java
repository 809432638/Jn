package client;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Put;
import util.HBaseInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class CellsScanners {
    public static void main(String[] args) throws IOException {
        Put put = new Put(HBaseInfo.ROW1)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V3);

        CellScanner scanner = put.cellScanner();
        while (scanner.advance()) {
            System.out.println(scanner.current());
        }
    }
}
