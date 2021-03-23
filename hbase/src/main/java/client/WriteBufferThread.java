package client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Put;
import util.HBaseInfo;
import util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WriteBufferThread {
    public static final Log LOG = LogFactory.getLog(WriteBufferThread.class);
    public static final int POOL_SIZE = 4;
    public static final int TASK_COUNT = 10;

    public static void main(String[] args) throws IOException {
        // create the test table ispace:itable with cf1,cf2 and max versions 3
        new CreateTestTable().runCreate();
        TableName tableName = TableName.valueOf("ispace:itable");

        BufferedMutator.ExceptionListener listener =
                (exception, mutator) -> {
                    for (int i = 0; i < exception.getNumExceptions(); i++) {
                        LOG.info("failed to send mutation: " + exception.getRow(i));
                    }
                };
        BufferedMutatorParams params =
                new BufferedMutatorParams(tableName).listener(listener);

        try (HBaseUtil util =
                HBaseUtil.getInstance(HBaseConfiguration.create());
                BufferedMutator mutator =
                        util.getConnection().getBufferedMutator(params)) {
            // Pool Create a worker pool to update the shared mutator in parallel
            ExecutorService workerpool =
                    Executors.newFixedThreadPool(POOL_SIZE);
            List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

            for (int i = 0; i < TASK_COUNT; i++) {
                futures.add(workerpool.submit(() -> {
                    Put put = new Put(HBaseInfo.ROW1)
                            .addColumn(HBaseInfo.CF1, HBaseInfo.F1C1,
                                    HBaseInfo.V1);
                    // Put Each worker uses the shared mutator instance,
                    // sharing the same backing buffer, callback listener,
                    // and RPC execuor pool
                    mutator.mutate(put);
                    // ... other works, maybe mutator.flush()
                    return null;
                }));
            }

            for(Future<Void> future : futures) {
                future.get(5, TimeUnit.MINUTES);
            }
            workerpool.shutdown();
        } catch (Exception e) {
            LOG.info("Exception when creating or freeing resources", e);
        }
    }
}
