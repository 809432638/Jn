package client;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BufferedMutator 有两个mutate方法，分别针对单个mutation和mutation list<br/>
 * 两个测试方法分别针对这两个mutate方法<br/>
 * 和 put / put list 用法完全一样<br/>
 * <br/>
 * 其实put等类继承了mutation，mutation又实现了Row接口，所以使用方式和put and puts类似<br/>
 * 使用put等直接用 table.put 就可以<br/>
 * 使用mutator要先调用mutate方法加载，然后调用flush() 方法将数据持久化<br/>
 * <br/>
 * 注意，此类使用场景不多，实际使用中：<br/>
 *   1. 如果要针对同一行进行多次操作，并且要求操作顺序，考虑使用mutateRow<br/>
 *   2. 如果有针对同一个表的不同记录的多个操作，考虑使用table.batch()<br/>
 *      2.1 注意，table.batch() 无法保证操作列表的顺序
 */
public class WriteBuffer {
    public static void main(String[] args) throws IOException {
        // rebuild table ispace:itable
        new CreateTestTable().runCreate();

        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create())) {
            System.out.println(StringUtils.rightPad("-", 120, "-"));
            System.out.println("single mutate");
            System.out.println(StringUtils.rightPad("-", 120, "-"));
            BufferedMutator bufferedMutator =
                    util.getConnection().getBufferedMutator(TableName.valueOf("ispace:itable"));
            Put put1 = new Put(HBaseInfo.ROW1)
                        .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V1)
                        .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V2);
            bufferedMutator.mutate(put1);
            Put put2 = new Put(HBaseInfo.ROW2)
                        .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1, HBaseInfo.V3)
                        .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2, HBaseInfo.V4);
            bufferedMutator.mutate(put2);

            System.out.println("scan table before flush:");
            util.getFullScanData(TableName.valueOf("ispace:itable"));

            System.out.println(StringUtils.rightPad("-", 120, "-"));
            System.out.println("scan table after flush:");
            bufferedMutator.flush();
            util.getFullScanData(TableName.valueOf("ispace:itable"));

            System.out.println(StringUtils.rightPad("-", 120, "-"));
            System.out.println("multiple mutate");
            System.out.println(StringUtils.rightPad("-", 120, "-"));

            BufferedMutator bufferedMutator1 =
                    util.getConnection().getBufferedMutator(TableName.valueOf("ispace:itable"));
            List<Mutation> mutationList = new ArrayList<>();

            Put put3 = new Put(HBaseInfo.ROW3)
                        .addColumn(HBaseInfo.CF1, HBaseInfo.F1C3, HBaseInfo.V5);
            mutationList.add(put3);
            Delete delete = new Delete(HBaseInfo.ROW1)
                            .addColumn(HBaseInfo.CF1, HBaseInfo.F1C2);
            mutationList.add(delete);

            bufferedMutator1.mutate(mutationList);

            System.out.println("scan table before flush:");
            util.getFullScanData(TableName.valueOf("ispace:itable"));
            System.out.println(StringUtils.rightPad("-", 120, "-"));
            System.out.println("scan table after flush:");
            bufferedMutator1.flush();
            util.getFullScanData(TableName.valueOf("ispace:itable"));
        }
    }
}
