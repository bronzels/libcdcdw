package at.bronzels.libcdcdw.kudu;

import at.bronzels.libcdcdw.Constants;
import at.bronzels.libcdcdw.OperationType;
import at.bronzels.libcdcdw.kudu.tool.KuduAgent;
import at.bronzels.libcdcdw.kudu.tool.KuduColumn;
import at.bronzels.libcdcdw.util.MyBson;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class KuduOperation {
    private static Logger LOGGER = LoggerFactory.getLogger(KuduOperation.class);

    static public String getResponseStr(OperationResponse operationResponse) {
        String ret = "success";
        if (operationResponse.hasRowError()) {
            Status status = operationResponse.getRowError().getErrorStatus();
            ret = "{\"code:\"" + status.getPosixCode() + ", \"status:\"" + status.toString() + "}";
        }
        return ret;
    }

    static public Operation getSetOnInsertEmulatedOperation(KuduTable kuduTable, KuduClient kuduClient, String _idValue, BsonDocument setOnInsertDoc, boolean isSrcFieldNameWTUpperCase) {
        KuduAgent agent = new KuduAgent();
        List<KuduColumn> list = new ArrayList<>();

        KuduColumn columnRK = new KuduColumn();
        columnRK.setColumnName(Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME).setColumnType(Type.STRING).setSelect(true).setComparisonOp(KuduPredicate.ComparisonOp.EQUAL).setComparisonValue(_idValue);
        list.add(columnRK);

        List<Map<String, Object>> selected = agent.select(kuduTable.getName(), kuduClient, list);

        Operation ret = null;
        if(selected.size() == 0) {
            BsonDocument rkDoc = new BsonDocument();
            rkDoc.put(Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME, new BsonString(_idValue));
            BsonDocument mergedDoc = MyBson.getMerged(rkDoc, setOnInsertDoc);
            ret = getOperation(OperationType.CREATE, kuduTable, mergedDoc, isSrcFieldNameWTUpperCase);
        }

        return ret;
    }

    static public Operation getNewOperation(OperationType operationType, KuduTable collection) {
        Operation ret;
        switch (operationType) {
            case CREATE:
                //operation = collection.newInsert();
                ret = collection.newUpsert();
                break;
            case UPDATE:
                //operation = collection.newUpdate();
                ret = collection.newUpsert();
                break;
            case DELETE:
                ret = collection.newDelete();
                break;
            default:
                ret = null;
                break;
        }
        return ret;
    }


    static public void haveRowAddedBy(PartialRow row, BsonDocument doc, boolean isSrcFieldNameWTUpperCase) {
        Set<String> keys = doc.keySet();
        for (String key : keys) {
            BsonValue value = doc.get(key);
            /*
            Field field = valueSchema.field(key);
            Schema.Type type = field.schema().type();
            if(!type.isPrimitive())
                throw new DataException("key:" + key + ", of doc:" + doc + " is not primitive");
             */
            Object javaValue = MyBson.getJavaObjFrom(key, value);
            //if(javaValue != null)
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            row.addObject(realKey4SinkDestination, javaValue);
        }
    }

    static public Operation getOperation(OperationType operationType, KuduTable collection, BsonDocument doc2FilterOrAdd, boolean isSrcFieldNameWTUpperCase) {
        Operation operation = getNewOperation(operationType, collection);
        if(operation != null) {
            PartialRow row = operation.getRow();
            if(operationType != OperationType.DELETE) {
                long currmilli = Instant.now().toEpochMilli();
                row.addObject(Constants.FIELDNAME_MODIFIED_TS, currmilli);
            }
            haveRowAddedBy(row, doc2FilterOrAdd, isSrcFieldNameWTUpperCase);
        }
        return operation;
    }

    static public void haveRowAddedBy(PartialRow row, Map<String, Object> map, boolean isSrcFieldNameWTUpperCase) {
        Set<String> keys = map.keySet();
        for (String key : keys) {
            Object value = map.get(key);
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            row.addObject(realKey4SinkDestination, value);
        }
    }

    static public void haveRowAddedBy(PartialRow row, Map<Integer, Object> map) {
        Set<Integer> keys = map.keySet();
        for (Integer index : keys) {
            Object value = map.get(index);
            row.addObject(index, value);
        }
    }

    static public void haveRowAddedByStrAsKey(PartialRow row, Map<String, Object> map) {
        Set<String> keys = map.keySet();
        for (String key : keys) {
            Object value = map.get(key);
            row.addObject(key, value);
        }
    }

    static public Operation getOperation(OperationType operationType, KuduTable collection, Map<String, Object> valueMap, String dwsynctsFieldName, boolean isSrcFieldNameWTUpperCase) {
        Operation operation = getNewOperation(operationType, collection);
        if(operation != null) {
            PartialRow row = operation.getRow();
            if(operationType != OperationType.DELETE && dwsynctsFieldName != null) {
                long currmilli = Instant.now().toEpochMilli();
                row.addObject(dwsynctsFieldName, currmilli);
            }
            haveRowAddedBy(row, valueMap, isSrcFieldNameWTUpperCase);
        }
        return operation;
    }

    static public Operation getOperation(OperationType operationType, KuduTable collection, Map<Integer, Object> valueMap, Integer tsFieldIndex) {
        Operation operation = getNewOperation(operationType, collection);
        if(operation != null) {
            PartialRow row = operation.getRow();
            if(operationType != OperationType.DELETE && tsFieldIndex != null) {
                long currmilli = Instant.now().toEpochMilli();
                row.addObject(tsFieldIndex, currmilli);
            }
            haveRowAddedBy(row, valueMap);
        }
        return operation;
    }

    static public Operation getOperationStrAsKey(OperationType operationType, KuduTable collection, Map<String, Object> valueMap, Integer tsFieldIndex) {
        Operation operation = getNewOperation(operationType, collection);
        if(operation != null) {
            PartialRow row = operation.getRow();
            if(operationType != OperationType.DELETE && tsFieldIndex != null) {
                long currmilli = Instant.now().toEpochMilli();
                row.addObject(tsFieldIndex, currmilli);
            }
            haveRowAddedByStrAsKey(row, valueMap);
        }
        return operation;
    }

    static public List<Map<String, Object>> getSelected(KuduClient kuduClient, KuduTable kuduTable, Map<String, Object> conditionMap, Set<String> additionalFieldSet) {
        KuduAgent agent = new KuduAgent();
        List<KuduColumn> list = new ArrayList<>();

        Set<String> keySet = conditionMap.keySet();
        for(String fieldName: keySet) {
            KuduColumn column2Query = new KuduColumn();
            Type type = KuduType.getColumnType(kuduTable, fieldName);
            column2Query.setColumnName(fieldName).setSelect(true).setColumnType(type).setComparisonOp(KuduPredicate.ComparisonOp.EQUAL).setComparisonValue(conditionMap.get(fieldName));
            list.add(column2Query);
        }

        if(additionalFieldSet != null) {
            for(String fieldName: additionalFieldSet) {
                KuduColumn column2Query = new KuduColumn();
                Type type = KuduType.getColumnType(kuduTable, fieldName);
                column2Query.setColumnName(fieldName).setSelect(true).setNullAble(true).setColumnType(type);
                list.add(column2Query);
            }
        }
        List<Map<String, Object>> selected = agent.select(kuduTable.getName(), kuduClient, list);

        return selected;
    }

    static private io.vavr.Tuple2<Map<String, Type>, List<Map<String, Object>>> getTypeSelectedMapTuple(KuduClient kuduClient, KuduTable kuduTable, Map<String, Object> conditionMap, Set<String> additionalFieldSet) {
        KuduAgent agent = new KuduAgent();
        Map<String, Type> fieldName2TypeMap = new HashMap<>();
        List<KuduColumn> list = new ArrayList<>();

        Set<String> keySet = conditionMap.keySet();
        for(String fieldName: keySet) {
            KuduColumn column2Query = new KuduColumn();
            Type type = KuduType.getColumnType(kuduTable, fieldName);
            fieldName2TypeMap.put(fieldName, type);
            column2Query.setColumnName(fieldName).setSelect(true).setColumnType(type).setComparisonOp(KuduPredicate.ComparisonOp.EQUAL).setComparisonValue(conditionMap.get(fieldName));
            list.add(column2Query);
        }

        if(additionalFieldSet != null) {
            for(String fieldName: additionalFieldSet) {
                KuduColumn column2Query = new KuduColumn();
                Type type = KuduType.getColumnType(kuduTable, fieldName);
                fieldName2TypeMap.put(fieldName, type);
                column2Query.setColumnName(fieldName).setSelect(true).setColumnType(type);
                list.add(column2Query);
            }
        }
        List<Map<String, Object>> selected = agent.select(kuduTable.getName(), kuduClient, list);

        return new io.vavr.Tuple2<Map<String, Type>, List<Map<String, Object>>>(fieldName2TypeMap, selected);
    }

    static public Operation getIncrEmulatedOperation(KuduTable kuduTable, KuduClient kuduClient, Map<String, Object> keyMap, Map<String, Object> valueMap, boolean isSrcFieldNameWTUpperCase) {
        io.vavr.Tuple2<Map<String, Type>, List<Map<String, Object>>> typeSelectedMapTuple = getTypeSelectedMapTuple(kuduClient, kuduTable, keyMap, valueMap.keySet());
        Map<String, Type> fieldName2TypeMap = typeSelectedMapTuple._1;
        List<Map<String, Object>> selected = typeSelectedMapTuple._2;
        Set<String> valueNameSet = valueMap.keySet();

        Operation ret;
        if(selected.size() == 0) {
            Map<String, Object> mergedMap = new HashMap(keyMap);
            mergedMap.putAll(valueMap);
            ret = KuduOperation.getOperation(OperationType.CREATE, kuduTable, mergedMap, at.bronzels.libcdcdw.Constants.FIELDNAME_MODIFIED_TS, isSrcFieldNameWTUpperCase);
        } else {
            Map<String, Object> retMap = selected.get(0);
            ret = kuduTable.newUpdate();
            PartialRow row = ret.getRow();

            for(Map.Entry<String, Object> entry: valueMap.entrySet()) {
                row.addObject(entry.getKey(), entry.getValue());
            }

            for(String fieldName : valueNameSet) {
                Object incrValue = valueMap.get(fieldName);
                Object oldValue = retMap.getOrDefault(fieldName, null);
                Object newValue;
                if(oldValue == null)
                    newValue = incrValue;
                else {
                    Type type = fieldName2TypeMap.get(fieldName);
                    newValue = KuduValue.getObjIncred(type, oldValue, incrValue);
                    if(newValue == null)
                        throw new RuntimeException(String.format("unrecognized type：%s, oldValue:%s, incrValue:%s",type, oldValue, incrValue));
                }
                String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? fieldName.toLowerCase() : fieldName;
                row.addObject(realKey4SinkDestination, newValue);
            }
            long currmilli = Instant.now().toEpochMilli();
            row.addObject(Constants.FIELDNAME_MODIFIED_TS, currmilli);
        }

        return ret;
    }

    static public Operation getIncrEmulatedOperation(KuduTable kuduTable, KuduClient kuduClient, BsonDocument keyDoc, BsonDocument incrDoc, boolean isSrcFieldNameWTUpperCase) {
        return getIncrEmulatedOperation(kuduTable, kuduClient, MyBson.getMap(keyDoc), MyBson.getMap(incrDoc), isSrcFieldNameWTUpperCase);
    }

    static public Operation getIncrEmulatedOperation(KuduTable kuduTable, KuduClient kuduClient, Map<String, Object> keyMap, BsonDocument incrDoc, boolean isSrcFieldNameWTUpperCase) {
        return getIncrEmulatedOperation(kuduTable, kuduClient, keyMap, MyBson.getMap(incrDoc), isSrcFieldNameWTUpperCase);
    }

    static public Operation getSetOnInsertEmulatedOperation(KuduTable kuduTable, KuduClient kuduClient, Map<String, Object> keyMap, Map<String, Object> valueMap, boolean isSrcFieldNameWTUpperCase) {
        List<Map<String, Object>> selected = getSelected(kuduClient, kuduTable, keyMap, null);

        Operation ret = null;
        if(selected.size() == 0) {
            Map<String, Object> mergedMap = new HashMap(keyMap);
            mergedMap.putAll(valueMap);
            ret = KuduOperation.getOperation(OperationType.CREATE, kuduTable, mergedMap, at.bronzels.libcdcdw.Constants.FIELDNAME_MODIFIED_TS, isSrcFieldNameWTUpperCase);
        }

        return ret;
    }

    static public Operation getUnSetEmulatedOperation(KuduTable kuduTable, KuduClient kuduClient, Map<String, Object> keyMap, Set<String> valueKey2Unset, boolean isSrcFieldNameWTUpperCase) {
        Operation ret = kuduTable.newUpdate();
        PartialRow row = ret.getRow();
        for (String key : keyMap.keySet()) {
            Object value = keyMap.get(key);
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            row.addObject(realKey4SinkDestination, value);
        }
        for (String key : valueKey2Unset) {
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            row.addObject(realKey4SinkDestination, null);
        }

        return ret;
    }

}
