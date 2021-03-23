package client;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Put;
import util.HBaseInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class CellsComparator {
    public static void main(String[] args) throws IOException {
        Put put1 = new Put(HBaseInfo.ROW3)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1);
        Put put2 = new Put(HBaseInfo.ROW1)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V3);
        Put put3 = new Put(HBaseInfo.ROW2)
                .addColumn(HBaseInfo.CF1, HBaseInfo.F1C5, HBaseInfo.V5);

        ArrayList<Cell> cells = new ArrayList<>();
        Put[] puts = new Put[]{put1, put2, put3};

        for(Put put : puts) {
            CellScanner scanner = put.cellScanner();
            while (scanner.advance()) {
                cells.add(scanner.current());
            }
        }

        System.out.println(" cells.get(0) sample ...");
        System.out.println("rowarray: " + cells.get(0).getRowArray() +
                ", rowoffset: " + cells.get(0).getRowOffset() +
                ", rowlength: " + cells.get(0).getRowLength());

        System.out.println(" shuffling ... ");
        Collections.shuffle(cells);
        cells.forEach(System.out::println);

        System.out.println(" Sorting ...");
        CellComparator cellComparator = new CellComparator.RowComparator();
        Collections.sort(cells, cellComparator);
        cells.forEach(System.out::println);
    }
}
