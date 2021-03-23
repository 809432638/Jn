package util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

public class HBaseUtil implements Closeable {
    private Configuration conf = null;
    private Connection conn = null;
    private Admin admin = null;

    private HBaseUtil(Configuration configuration) throws IOException {
        this.conf = configuration;
        this.conn = ConnectionFactory.createConnection(conf);
        this.admin = conn.getAdmin();
    }

    /**
     * @param configuration files, eg: hbase-site.xml, core-site.xml
     * @param configFiles
     * @throws URISyntaxException
     * @throws IOException
     */
    private HBaseUtil(Configuration configuration, String[] configFiles) throws URISyntaxException, IOException {
        this.conf = configuration;
        for (String file : configFiles) {
            this.conf.addResource(new Path(ClassLoader.getSystemResource(file).toURI()));
        }
        this.conn = ConnectionFactory.createConnection(this.conf);
        this.admin = conn.getAdmin();
    }

    public static HBaseUtil getInstance(Configuration conf) throws IOException {
        return new HBaseUtil(conf);
    }

    public static HBaseUtil getInstance(Configuration conf,
            String[] configFiles) throws IOException, URISyntaxException {
        return new HBaseUtil(conf, configFiles);
    }

    public Configuration getConfigration() {
        return this.conf;
    }

    ;

    public Connection getConnection() {
        return this.conn;
    }

    public boolean isTableExists(String tableName) throws IOException {
        return isTableExists(TableName.valueOf(tableName));
    }

    public boolean isTableExists(byte[] tableName) throws IOException {
        return isTableExists(TableName.valueOf(tableName));
    }

    /**
     * @param name table name, type:org.apache.hadoop.hbase.TableName
     * @return
     * @throws IOException
     */
    public boolean isTableExists(TableName name) throws IOException {
        return admin.tableExists(name);
    }

    public boolean createNamespace(String spaceName) {
        return createNamespace(spaceName, null);
    }

    /**
     * @param spaceName
     * @param params
     * @return
     */
    public boolean createNamespace(String spaceName,
            Map<String, String> params) {
        try {
            NamespaceDescriptor descriptor = null;
            if (params == null || params.isEmpty()) {
                descriptor = NamespaceDescriptor.create(spaceName).build();
            } else {
                descriptor = NamespaceDescriptor.create(spaceName)
                        .addConfiguration(params).build();
            }
            admin.createNamespace(descriptor);
            return true;
        } catch (Exception e) {
            System.out.println("Error when create namespace : " + e.getMessage());
            return false;
        }
    }

    /**
     * to drop namespace, delete tables when set force to "true"
     *
     * @param spaceName
     * @param force
     * @return
     */
    public boolean dropNamespace(String spaceName, boolean force) {
        boolean result = true;
        // delete tables in namespace when set force to "true"
        if (force) {
            try {
                TableName[] tableNames =
                        admin.listTableNamesByNamespace(spaceName);
                for (TableName tableName : tableNames) {
                    deleteTable(tableName);
                }
            } catch (NamespaceNotFoundException e) {
                // do nothing
            } catch (Exception e) {
                writeLog("Error when delete tables: " + e.getMessage());
                result = false;
                return result;
            }
        }

        // delete namespace
        try {
            this.admin.deleteNamespace(spaceName);
        } catch (NamespaceNotFoundException e) {
            writeLog("Namespace: <" + spaceName + "> not found when delete.");
            result = true;
        } catch (Exception e) {
            writeLog("Error when delete namespace: " + e.getMessage());
            result = false;
        }

        return result;
    }

    public void createTable(String tableName, String... families) throws IOException {
        createTable(TableName.valueOf(tableName), 3, null, families);
    }

    public void createTable(TableName tableName, String... families) throws IOException {
        createTable(tableName, 3, null, families);
    }

    public void createTable(String tableName, int maxVersions,
            String... families) throws IOException {
        createTable(TableName.valueOf(tableName), maxVersions, null, families);
    }

    public void createTable(TableName tableName, int maxVersions,
            String... families) throws IOException {
        createTable(tableName, maxVersions, null, families);
    }

    public void createTable(String tableName, byte[][] splitKeys,
            String... families) throws IOException {
        createTable(TableName.valueOf(tableName), 3, splitKeys, families);
    }

    public void createTable(TableName tableName, byte[][] splitKeys,
            String... families) throws IOException {
        createTable(tableName, 3, splitKeys, families);
    }

    /**
     * @param tableName
     * @param maxVersions : default as 3
     * @param splitKeys
     * @param families    : list of column families
     * @throws IOException
     */
    public void createTable(TableName tableName, int maxVersions,
            byte[][] splitKeys, String... families) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        Arrays.asList(families).forEach(x -> {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(x);
            columnDescriptor.setMaxVersions(StringUtils
                    .isEmpty(String.valueOf(maxVersions)) ? 1 : maxVersions);
            tableDescriptor.addFamily(columnDescriptor);
        });

        if (splitKeys != null) {
            this.admin.createTable(tableDescriptor, splitKeys);
        } else {
            this.admin.createTable(tableDescriptor);
        }
    }

    public Table getTable(String tableName) throws IOException {
        return getTable(TableName.valueOf(tableName));
    }

    public Table getTable(TableName tableName) throws IOException {
        return this.conn.getTable(tableName);
    }

    public void getTableInfo(String tableName) throws IOException {
        getTableInfo(TableName.valueOf(tableName));
    }

    public void getTableInfo(TableName tableName) throws IOException {
        HTableDescriptor tableDescriptor =
                this.admin.getTableDescriptor(tableName);
        // add handle process later...
        writeLog(tableDescriptor.toString());
    }

    public boolean disableTable(String tableName) throws IOException {
        return disableTable(TableName.valueOf(tableName));
    }

    public boolean disableTable(TableName tableName) throws IOException {
        boolean resultBoolean = true;
        try {
            if (this.admin.isTableAvailable(tableName)) {
                if (this.admin.isTableEnabled(tableName)) {
                    this.admin.disableTable(tableName);
                }
            } else {
                writeLog("Table <" + tableName.toString() + "> is not " +
                        "available.");
            }
        } catch (Exception e) {
            writeLog("Error when disable table: <" + tableName.toString() +
                    "> , " + e.getMessage());
            resultBoolean = false;
        } finally {
            return resultBoolean;
        }
    }

    public boolean deleteTable(String tableName) throws IOException {
        return deleteTable(TableName.valueOf(tableName));
    }

    /**
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean deleteTable(TableName tableName) throws IOException {
        if (this.disableTable(tableName)) {
            try {
                this.admin.deleteTable(tableName);
                return true;
            } catch (Exception e) {
                writeLog("Error when delete table:<" + tableName.toString()
                        + "> , " + e.getMessage());
                return false;
            }
        } else {
            return false;
        }
    }

    public void getFullScanData(String tableName) throws IOException {
        getFullScanData(TableName.valueOf(tableName));
    }

    /**
     * get full data(with all versions) for the table
     *
     * @param tableName
     * @throws IOException
     */
    public void getFullScanData(TableName tableName) throws IOException {
        try (Table table = this.conn.getTable(tableName);
                ResultScanner scanner =
                        table.getScanner(new Scan().setMaxVersions())) {
            for (Result result : scanner) {
                getResultData(result);
            }
        }
    }

    public void getNormalScanData(String tableName) throws IOException {
        getNormalScanData(TableName.valueOf(tableName));
    }

    /**
     * get normal data(latest version) for the table
     *
     * @param tableName
     * @throws IOException
     */
    public void getNormalScanData(TableName tableName) throws IOException {
        try (Table table = this.conn.getTable(tableName);
                ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                getResultData(result);
            }
        }
    }

    /**
     * get all cell's row/cf/qual/ts/value
     *
     * @param result
     */
    public void getResultData(Result result) {
        //writeLog("Cell\t|\tRow\t|\tColFamlily\t|\tQual\t|\tts\t|\tValue");
        System.out.println("");
        writeLog(StringUtils.rightPad("Cell", 49, " ") + "|" +
                StringUtils.rightPad("Row", 19, " ") + "|" +
                StringUtils.rightPad("Family", 7, " ") + "|" +
                StringUtils.rightPad("Qualifier", 11, " ") + "|" +
                StringUtils.rightPad("Timestamp", 15, " ") + "|" +
                StringUtils.rightPad("Value", 14, " "));
        writeLog(StringUtils.rightPad("-", 49, "-") + "|" +
                StringUtils.rightPad("-", 19, "-") + "|" +
                StringUtils.rightPad("-", 7, "-") + "|" +
                StringUtils.rightPad("-", 11, "-") + "|" +
                StringUtils.rightPad("-", 15, "-") + "|" +
                StringUtils.rightPad("-", 14, "-"));
        for (Cell cell : result.rawCells()) {
            writeLog(StringUtils.rightPad(cell.toString(), 49, " ") + "|" +
                    StringUtils.rightPad(Bytes.toString(CellUtil.cloneRow(cell)), 19, " ") + "|" +
                    StringUtils.rightPad(Bytes.toString(CellUtil.cloneFamily(cell)), 7, " ") + "|" +
                    StringUtils.rightPad(Bytes.toString(CellUtil.cloneQualifier(cell)), 11, " ") + "|" +
                    StringUtils.rightPad(String.valueOf(cell.getTimestamp()), 15,
                            " ") + "|" +
                    StringUtils.rightPad(Bytes.toString(CellUtil.cloneValue(cell)), 15, " "));
            /*
            writeLog(cell +
                    "\t|\t" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    "\t|\t" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    "\t|\t" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    "\t|\t" + cell.getTimestamp() +
                    "\t|\t" + Bytes.toString(CellUtil.cloneValue(cell)));
             */
        }
    }

    private void writeLog(String log) {
        writeLog(log, null);
    }

    /**
     * Level: DEBUG, INFO, WARN, ERROR
     *
     * @param log
     * @param Level
     */
    private void writeLog(String log, String Level) {
        System.out.println(log);
    }

    public boolean isClosed() {
        return this.conn.isClosed();
    }

    @Override
    public void close() throws IOException {
        this.conn.close();
        // this.admin.close();
    }
}
