package at.bronzels.libcdcdw.kudu.pool;

import at.bronzels.libcdcdw.Constants;
import at.bronzels.libcdcdw.OperationType;
import at.bronzels.libcdcdw.kudu.KuduOperation;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyKudu implements Serializable {
    private final Logger LOG = LoggerFactory.getLogger(MyKudu.class);

    private String catalog;
    private String kuduUrl;
    private String kuduDatabase;
    private String tableName;

    private KuduClient kuduClient = null;
    private KuduSession kuduSession = null;

    protected KuduTable kuduTable = null;

    private Map<String, Integer> name2IndexMap;

    public MyKudu(String catalog, String kuduUrl, String kuduDatabase, String tableName) {
        this.catalog = catalog;
        this.kuduUrl = kuduUrl;
        this.kuduDatabase = kuduDatabase;
        this.tableName = tableName;
    }

    protected void finalize( )
    {
// finalization code here
        close();
    }

    public void open() {
        kuduClient = new KuduClient.KuduClientBuilder(kuduUrl).defaultAdminOperationTimeoutMs(600000).build();

        kuduSession = kuduClient.newSession();
        kuduSession.setTimeoutMillis(60000);
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        try {
            String name = catalog + Constants.KUDU_TABLE_NAME_AFTER_CATALOG_SEP + kuduDatabase + Constants.KUDU_TABLE_NAME_SEP + tableName;
            kuduTable = kuduClient.openTable(name);
            name2IndexMap = instantiateName2IndexMap();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Integer> instantiateName2IndexMap() {
        Map<String, Integer> ret = new HashMap<>();
        Schema schema = kuduTable.getSchema();
        List<ColumnSchema> columnSchemaList = schema.getColumns();
        int size = columnSchemaList.size();
        for (int i = 0; i < size; i++) {
            String name = columnSchemaList.get(i).getName();
            ret.put(name, schema.getColumnIndex(name));
        }
        return ret;
    }

    public Map<String, Integer> getName2IndexMap() {
        return name2IndexMap;
    }

    public KuduTable getKuduTable() {
        return kuduTable;
    }

    public KuduSession getKuduSession() {
        return kuduSession;
    }

    public KuduClient getKuduClient() {
        return kuduClient;
    }

    public void applyOp(Operation op) {
        try {
            OperationResponse operationResponse = kuduSession.apply(op);
            LOG.info("operationResponse:{}", operationResponse);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void put(Map<Integer, Object> valueMap, Integer dwsynctsFieldIndex) {
        Operation op = KuduOperation.getOperation(OperationType.UPDATE, kuduTable, valueMap, dwsynctsFieldIndex);
        applyOp(op);
    }

    public void put(Map<Integer, Object> valueMap) {
        Operation op = KuduOperation.getOperation(OperationType.UPDATE, kuduTable, valueMap, null);
        applyOp(op);
    }

    public void delete(Map<Integer, Object> valueMap) {
        Operation op = KuduOperation.getOperation(OperationType.DELETE, kuduTable, valueMap, null);
        applyOp(op);
    }

    public void close() {
        try {
            if (kuduSession != null)
                kuduSession.close();
            if (kuduClient != null)
                kuduClient.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

}
