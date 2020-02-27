package at.bronzels.libcdcdw.kudu.pool;

import at.bronzels.libcdcdw.Constants;
import at.bronzels.libcdcdw.DistLockRedisson;
import at.bronzels.libcdcdw.OperationType;
import at.bronzels.libcdcdw.DistLock;
import at.bronzels.libcdcdw.conf.DistLockConf;
import at.bronzels.libcdcdw.kudu.KuduOperation;

import at.bronzels.libcdcdw.kudu.tool.KuduAgent;
import at.bronzels.libcdcdw.kudu.tool.KuduColumn;
import at.bronzels.libcdcdw.kudu.tool.KuduRow;

import static at.bronzels.libcdcdw.util.MyCollection.*;

import at.bronzels.libcdcdw.util.MyCollection;
import at.bronzels.libcdcdw.util.MyString;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class MyKudu implements Serializable {
    private final Logger LOG = LoggerFactory.getLogger(MyKudu.class);

    private String catalog;
    private String kuduUrl;
    private String kuduDatabase;
    private String tableName;

    private boolean isInAutoMode = true;

    private KuduClient kuduClient;
    private KuduSession kuduSession;

    protected KuduTable kuduTable;

    private String strFullTableName;

    private Map<String, Integer> name2IndexMap;
    private Set<String> colNameSet;
    private Map<String, Type> name2TypeMap;

    private Map<Integer, Type> index2TypeMap;

    private Integer dwsynctsFieldIndex = null;

    private Set<String> primaryKeyNameSet;
    private Set<Integer> primaryKeyIndexSet;

    private boolean isSrcFieldNameWTUpperCase = false;

    private DistLockConf distLockConf;
    private DistLock distLock;

    public MyKudu(String catalog, String kuduUrl, String kuduDatabase, String tableName) {
        this.catalog = catalog;
        this.kuduUrl = kuduUrl;
        this.kuduDatabase = kuduDatabase;
        this.tableName = tableName;
    }

    public MyKudu(String catalog, String kuduUrl, String kuduDatabase, String tableName, DistLockConf distLockConf) {
        this(catalog, kuduUrl, kuduDatabase, tableName);
        this.distLockConf = distLockConf;
    }

    public MyKudu(boolean isSrcFieldNameWTUpperCase, String catalog, String kuduUrl, String kuduDatabase, String tableName) {
        this(catalog, kuduUrl, kuduDatabase, tableName);
        this.isSrcFieldNameWTUpperCase = isSrcFieldNameWTUpperCase;
    }

    public MyKudu(boolean isSrcFieldNameWTUpperCase, String catalog, String kuduUrl, String kuduDatabase, String tableName, DistLockConf distLockConf) {
        this(isSrcFieldNameWTUpperCase, catalog, kuduUrl, kuduDatabase, tableName);
        this.distLockConf = distLockConf;
    }

    public MyKudu(String catalog, String kuduUrl, String kuduDatabase, String tableName, boolean isInAutoMode) {
        this(catalog, kuduUrl, kuduDatabase, tableName);
        this.isInAutoMode = isInAutoMode;
    }

    public MyKudu(String catalog, String kuduUrl, String kuduDatabase, String tableName, boolean isInAutoMode, DistLockConf distLockConf) {
        this(catalog, kuduUrl, kuduDatabase, tableName, isInAutoMode);
        this.distLockConf = distLockConf;
    }

    public MyKudu(boolean isSrcFieldNameWTUpperCase, String catalog, String kuduUrl, String kuduDatabase, String tableName, boolean isInAutoMode) {
        this(catalog, kuduUrl, kuduDatabase, tableName, isInAutoMode);
        this.isSrcFieldNameWTUpperCase = isSrcFieldNameWTUpperCase;
    }

    public MyKudu(boolean isSrcFieldNameWTUpperCase, String catalog, String kuduUrl, String kuduDatabase, String tableName, boolean isInAutoMode, DistLockConf distLockConf) {
        this(isSrcFieldNameWTUpperCase, catalog, kuduUrl, kuduDatabase, tableName, isInAutoMode);
        this.distLockConf = distLockConf;
    }

    protected void finalize() {
// finalization code here
        close();
    }

    public void openDB() {
        kuduClient = new KuduClient.KuduClientBuilder(kuduUrl).defaultAdminOperationTimeoutMs(600000).build();

        kuduSession = kuduClient.newSession();
        kuduSession.setTimeoutMillis(60000);
        if (isInAutoMode)
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        else
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        try {
            strFullTableName = catalog + Constants.KUDU_TABLE_NAME_AFTER_CATALOG_SEP + kuduDatabase + Constants.KUDU_TABLE_NAME_SEP + tableName;

            kuduTable = kuduClient.openTable(strFullTableName);
            io.vavr.Tuple4<Map<String, Integer>, Map<String, Type>, Map<Integer, Type>, Set<String>> tuple4 = instantiateName2IndexTypeMapPrimaryKeySetTuple();
            name2IndexMap = tuple4._1;
            name2TypeMap = tuple4._2;
            index2TypeMap = tuple4._3;
            primaryKeyNameSet = tuple4._4;
            primaryKeyIndexSet = primaryKeyNameSet.stream()
                    .map(name2IndexMap::get)
                    .collect(Collectors.toSet());
            colNameSet = name2IndexMap.keySet();
            dwsynctsFieldIndex = name2IndexMap.get(Constants.FIELDNAME_MODIFIED_TS);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void open() {
        openDB();
        if (distLockConf != null) {
            String lockName = MyString.concatBySkippingEmpty(Constants.commonSep, distLockConf.getPrefix(), strFullTableName);
            distLock = new DistLockRedisson(distLockConf.getUrl(), lockName);
            //distLock = new DistLockZookeeper(distLockConf.getUrl(), lockName);
            distLock.open();
        }
    }

    public void closeDB() {
        try {
            if (kuduSession != null)
                kuduSession.close();
            if (kuduClient != null)
                kuduClient.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (distLock != null)
            distLock.close();
        closeDB();
    }

    private <T> Map<String, T> getMyLowerCasedMap(Map<String, T> input) {
        if (isSrcFieldNameWTUpperCase)
            return getLowerCasedMap(input);
        else return input;
    }

    private Set<String> getMyLowerCasedSet(Set<String> input) {
        if (isSrcFieldNameWTUpperCase)
            return getLowerCasedSet(input);
        else return input;
    }

    private io.vavr.Tuple4<Map<String, Integer>, Map<String, Type>, Map<Integer, Type>, Set<String>> instantiateName2IndexTypeMapPrimaryKeySetTuple() {
        Map<String, Integer> indexMap = new HashMap<>();
        Map<String, Type> typeMap = new HashMap<>();
        Map<Integer, Type> indexTypeMap = new HashMap<>();
        Set<String> primaryKeySet = new HashSet<>();
        Schema schema = kuduTable.getSchema();
        List<ColumnSchema> columnSchemaList = schema.getColumns();
        int size = columnSchemaList.size();
        for (int i = 0; i < size; i++) {
            ColumnSchema columnSchema = columnSchemaList.get(i);
            String name = columnSchema.getName();
            if (columnSchema.isKey())
                primaryKeySet.add(name);
            Type type = columnSchema.getType();
            indexMap.put(name, schema.getColumnIndex(name));
            typeMap.put(name, type);
            indexTypeMap.put(schema.getColumnIndex(name), type);
        }
        return new io.vavr.Tuple4<Map<String, Integer>, Map<String, Type>, Map<Integer, Type>, Set<String>>(indexMap, typeMap, indexTypeMap, primaryKeySet);
    }

    public Map<String, Integer> getName2IndexMap() {
        return name2IndexMap;
    }

    public Map<String, Type> getName2TypeMap() {
        return name2TypeMap;
    }

    public Set<String> getColNameSet() {
        return colNameSet;
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

    public String getStrFullTableName() {
        return strFullTableName;
    }

    public String applyOp(Operation op) {
        String ret = "success";
        try {
            OperationResponse operationResponse = kuduSession.apply(op);
            if(isInAutoMode) {
                if (operationResponse.hasRowError())
                    ret = KuduOperation.getResponseStr(operationResponse);
                LOG.info("operationResponse:{}", operationResponse);
            }
        } catch (KuduException e) {
            ret = e.getMessage();
            e.printStackTrace();
        }
        return ret;
    }

    public String flush() {
        String ret = "success";
        if (isInAutoMode)
            return ret;
        try {
            List<OperationResponse> resp = kuduSession.flush();
            ret = "\n";
            for (OperationResponse operationResponse : resp) {
                LOG.info("operationResponse:{}", operationResponse);
                ret += KuduOperation.getResponseStr(operationResponse) + "\n";
            }
        } catch (KuduException e) {
            ret = e.getMessage();
            e.printStackTrace();
        }
        return ret;
    }

    private Integer getTsIndex(Integer tsFieldIndex) {
        if (tsFieldIndex == null)
            return null;
        else {
            if (tsFieldIndex.equals(Constants.tsFieldIndexReusedDwsync))
                return dwsynctsFieldIndex;
            else return tsFieldIndex;
        }
    }

    public void addColumns(Map<String, Type> inputColName2TypeMap) {
        Map<String, Type> colName2TypeMap = getMyLowerCasedMap(inputColName2TypeMap);

        if (intersect(colName2TypeMap.keySet(), primaryKeyNameSet).size() > 0)
            throw new RuntimeException("overlapped with primary keys");
        KuduAgent agent = new KuduAgent();
        KuduRow myrows = new KuduRow();
        myrows.setTableName(strFullTableName);
        List<KuduColumn> columnList = new ArrayList<>();
        for (Map.Entry<String, Type> entry : colName2TypeMap.entrySet()) {
            KuduColumn newcolumn = new KuduColumn();
            newcolumn.setColumnName(entry.getKey()).setAlterColumnEnum(KuduColumn.AlterColumnEnum.ADD_COLUMN).setNullAble(true).setColumnType(entry.getValue());
            columnList.add(newcolumn);
        }
        myrows.setRows(columnList);
        List<KuduRow> list = new ArrayList<>();
        list.add(myrows);

        boolean stillNeeded = true;
        try {
            distLock.acquire();
            closeDB();
            openDB();
            if (colNameSet.containsAll(inputColName2TypeMap.keySet()))
                stillNeeded = false;
            if (stillNeeded) {
                for (KuduRow entity : list) {
                    AlterTableResponse alterTableResponse = agent.alterColumn(kuduClient, entity);
                }
                closeDB();
                openDB();
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("kudu执行表alter操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            throw new RuntimeException(e.getMessage());
        } finally {
            distLock.release();
        }
    }

    private void checkWithValueDataType(Map<Integer, Object> valueMap){
        valueMap.forEach((index, type) -> {

        });
    }

    private void checkWithPrimaryKeysByIndex(Map<Integer, Object> valueMap) {
        if (intersect(valueMap.keySet(), primaryKeyIndexSet).size() != primaryKeyIndexSet.size())
            throw new RuntimeException("not contain all primary keys");
    }

    private void checkWithPrimaryKeysByName(Map<String, Object> valueMap) {
        if (intersect(valueMap.keySet(), primaryKeyNameSet).size() != primaryKeyNameSet.size())
            throw new RuntimeException("not contain all primary keys");
    }

    private void checkExactPrimaryKeysByIndex(Map<Integer, Object> valueMap) {
        if (!isSame(valueMap.keySet(), primaryKeyIndexSet))
            throw new RuntimeException("not exact same to primary keys");
    }

    private void checkExactPrimaryKeysByName(Map<String, Object> valueMap) {
        if (!isSame(valueMap.keySet(), primaryKeyNameSet))
            throw new RuntimeException("not exact same to primary keys");
    }

    //no sql rk based op
    public String put(Map<Integer, Object> valueMap, Integer tsFieldIndex) {
        checkWithPrimaryKeysByIndex(valueMap);
        Operation op = KuduOperation.getOperation(OperationType.UPDATE, kuduTable, valueMap, getTsIndex(tsFieldIndex));
        return "put, " + applyOp(op);
    }

    public String putStrAsKey(Map<String, Object> inputValueMap, Integer tsFieldIndex, boolean isSrcFieldNameWTUpperCase) {
        Map<String, Object> valueMap = getMyLowerCasedMap(inputValueMap);
        checkWithPrimaryKeysByName(valueMap);
        Operation op = KuduOperation.getOperationStrAsKey(OperationType.UPDATE, kuduTable, valueMap, tsFieldIndex);
        return "put, " + applyOp(op);
    }

    public String putStrAsKey(Map<String, Object> inputValueMap) {
        Map<String, Object> valueMap = getMyLowerCasedMap(inputValueMap);
        checkWithPrimaryKeysByName(valueMap);
        Operation op = KuduOperation.getOperationStrAsKey(OperationType.UPDATE, kuduTable, valueMap, dwsynctsFieldIndex);
        return "put, " + applyOp(op);
    }

    public String put(Map<Integer, Object> valueMap) {
        checkWithPrimaryKeysByIndex(valueMap);
        Operation op = KuduOperation.getOperation(OperationType.UPDATE, kuduTable, valueMap, null);
        return "put, " + applyOp(op);
    }



    public String incr(Map<String, Object> inputKeyMap, Map<String, Object> inputValueMap) {
        Map<String, Object> keyMap = getMyLowerCasedMap(inputKeyMap);
        Map<String, Object> valueMap = getMyLowerCasedMap(inputValueMap);
        checkExactPrimaryKeysByName(keyMap);
        Operation op = KuduOperation.getIncrEmulatedOperation(kuduTable, kuduClient, keyMap, valueMap, isSrcFieldNameWTUpperCase);
        return "put, " + applyOp(op);
    }

    public String incr(Map<String, Object> keyAndValueMap) {
        io.vavr.Tuple2<Map<String, Object>, Map<String, Object>> tuple = splitKeyAndValue(keyAndValueMap);
        Map<String, Object> keyMap = getMyLowerCasedMap(tuple._1);
        Map<String, Object> valueMap = getMyLowerCasedMap(tuple._2);
        checkExactPrimaryKeysByName(keyMap);
        Operation op = KuduOperation.getIncrEmulatedOperation(kuduTable, kuduClient, keyMap, valueMap, isSrcFieldNameWTUpperCase);
        return "put, " + applyOp(op);
    }

    public String setOnInsert(Map<String, Object> inputKeyMap, Map<String, Object> inputValueMap, Integer tsFieldIndex, boolean isSrcFieldNameWTUpperCase) {
        Map<String, Object> keyMap = getMyLowerCasedMap(inputKeyMap);
        Map<String, Object> valueMap = getMyLowerCasedMap(inputValueMap);
        checkExactPrimaryKeysByName(keyMap);
        Operation op = KuduOperation.getSetOnInsertEmulatedOperation(kuduTable, kuduClient, keyMap, valueMap, isSrcFieldNameWTUpperCase);
        return "put, " + applyOp(op);
    }

    private io.vavr.Tuple2<Map<String, Object>, Map<String, Object>> splitKeyAndValue(Map<String, Object> keyAndValueMap) {
        Map<String, Object> keyMap = new HashMap<>();
        Map<String, Object> valueMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : keyAndValueMap.entrySet()) {
            String entryKey = entry.getKey();
            Object entryValue = entry.getValue();
            if (primaryKeyNameSet.contains(entryKey))
                keyMap.put(entryKey, entryValue);
            else
                valueMap.put(entryKey, entryValue);
        }
        return new io.vavr.Tuple2<Map<String, Object>, Map<String, Object>>(keyMap, valueMap);
    }

    public String setOnInsert4PropAvailable(Map<String, Object> keyAndValueMap) {
        io.vavr.Tuple2<Map<String, Object>, Map<String, Object>> tuple = splitKeyAndValue(keyAndValueMap);
        Map<String, Object> keyMap = tuple._1;
        checkExactPrimaryKeysByName(keyMap);
        Set<String> valueNameSet = tuple._2.keySet();
        List<Map<String, Object>> selected = getSelected(keyMap, valueNameSet);

        if (selected.size() == 0) {
            return "noexisted";
        } else {
            Map<String, Object> selectedByKey = selected.get(0);
            Set<String> validPropNameSet = selectedByKey.entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .map(Map.Entry::getKey)
                    .filter(name -> !primaryKeyNameSet.contains(name))
                    .collect(Collectors.toSet());
            if (validPropNameSet.size() == 0) {
                putStrAsKey(keyAndValueMap);
                return "success";
            } else return "exists but there are already valid props, not set";
        }
    }

    public String unset(Map<String, Object> keyAndValueMap) {
        io.vavr.Tuple2<Map<String, Object>, Map<String, Object>> tuple = splitKeyAndValue(keyAndValueMap);
        Map<String, Object> keyMap = tuple._1;
        checkExactPrimaryKeysByName(keyMap);
        Set<String> valueNameSet = tuple._2.keySet();

        Operation op = KuduOperation.getUnSetEmulatedOperation(kuduTable, kuduClient, keyMap, valueNameSet, isSrcFieldNameWTUpperCase);

        return "unset, " + applyOp(op);
    }

    public String delete(Map<Integer, Object> valueMap) {
        checkExactPrimaryKeysByIndex(valueMap);
        Operation op = KuduOperation.getOperation(OperationType.DELETE, kuduTable, valueMap, null);
        return "delete, " + applyOp(op);
    }

    public String deleteStrAsKey(Map<String, Object> inputValueMap) {
        Map<String, Object> valueMap = getMyLowerCasedMap(inputValueMap);
        checkExactPrimaryKeysByName(valueMap);
        Operation op = KuduOperation.getOperationStrAsKey(OperationType.DELETE, kuduTable, valueMap, null);
        return "delete, " + applyOp(op);
    }

    //use by both up/down side
    public List<Map<String, Object>> getSelected(Map<String, Object> inputConditionMap, Set<String> inputAdditionalFieldSet) {
        Map<String, Object> conditionMap = getMyLowerCasedMap(inputConditionMap);
        Set<String> additionalFieldSet = getMyLowerCasedSet(inputAdditionalFieldSet);
        return KuduOperation.getSelected(kuduClient, kuduTable, conditionMap, additionalFieldSet);
    }

    public List<Map<String, Object>> getSelectedAll(Map<String, Object> inputConditionMap) {
        Map<String, Object> conditionMap = getMyLowerCasedMap(inputConditionMap);
        Set<String> additionalFieldSet = MyCollection.minus(colNameSet, conditionMap.keySet());
        return KuduOperation.getSelected(kuduClient, kuduTable, conditionMap, additionalFieldSet);
    }

    //multiple or event bulk(explicit named) from now on

    public boolean isExisted(Map<String, Object> inputConditionMap) {
        Map<String, Object> conditionMap = getMyLowerCasedMap(inputConditionMap);
        return KuduOperation.getSelected(kuduClient, kuduTable, conditionMap, null).size() > 0;
    }

    private List<Map<String, Object>> getSelectedBeforeUpdateOn(Map<String, Object> conditionMap, Map<String, Object> updateMap) {
        Set<String> conditionKeySet = conditionMap.keySet();

        if (intersect(updateMap.keySet(), primaryKeyNameSet).size() != 0)
            throw new RuntimeException("primary keys can't be updated");

        Set<String> additionalFieldSet = null;
        if (!conditionKeySet.containsAll(primaryKeyNameSet)) {
            additionalFieldSet = new HashSet<String>(primaryKeyNameSet);
            additionalFieldSet.removeAll(conditionKeySet);
        }
        List<Map<String, Object>> selected = KuduOperation.getSelected(kuduClient, kuduTable, conditionMap, additionalFieldSet);

        return selected;
    }

    public String updateAfterSelected(boolean bulkOrNot, Map<String, Object> inputConditionMap, Map<String, Object> inputUpdateMap, Integer tsFieldIndex, boolean isSrcFieldNameWTUpperCase) {
        Map<String, Object> conditionMap = getMyLowerCasedMap(inputConditionMap);
        Map<String, Object> updateMap = getMyLowerCasedMap(inputUpdateMap);

        List<Map<String, Object>> selected = getSelectedBeforeUpdateOn(conditionMap, updateMap);

        int size = selected.size();
        if (size == 0)
            return "zero";
        StringBuilder ret = new StringBuilder().append(String.format("to update %d records\n", size));
        Integer finalTsIndex = getTsIndex(tsFieldIndex);
        MyKudu myKudu;
        if (bulkOrNot) {
            myKudu = new MyKudu(isSrcFieldNameWTUpperCase, catalog, kuduUrl, kuduDatabase, tableName, false, distLockConf);
            myKudu.open();
        } else myKudu = this;
        for (Map<String, Object> map : selected) {
            Map<String, Object> pkMap = map.entrySet().stream()
                    .filter(entry -> primaryKeyNameSet.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Map<String, Object> putMap = new HashMap<>(pkMap);
            putMap.putAll(updateMap);
            Operation operation = KuduOperation.getNewOperation(OperationType.UPDATE, myKudu.kuduTable);
            if (operation != null) {
                PartialRow row = operation.getRow();
                if (finalTsIndex != null) {
                    long currmilli = Instant.now().toEpochMilli();
                    //row.addLong(dwsynctsFieldIndex, currmilli);
                    row.addObject(dwsynctsFieldIndex, currmilli);
                }
                KuduOperation.haveRowAddedBy(row, putMap, myKudu.isSrcFieldNameWTUpperCase);
            }
            String resp = myKudu.applyOp(operation);
            if (!bulkOrNot)
                ret.append(resp);
        }
        if (bulkOrNot) {
            ret.append(myKudu.flush());
            myKudu.close();
        }
        return ret.toString();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
