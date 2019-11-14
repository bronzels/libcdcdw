package at.bronzels.libcdcdw.kudu;

import at.bronzels.libcdcdw.Constants;
import at.bronzels.libcdcdw.OperationType;
import at.bronzels.libcdcdw.kudu.tool.KuduAgent;
import at.bronzels.libcdcdw.kudu.tool.KuduColumn;
import at.bronzels.libcdcdw.util.MyBson;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

public class KuduOperation {
    private static Logger LOGGER = LoggerFactory.getLogger(KuduOperation.class);

    static public Operation getIncrEmulatedOperation(KuduTable kuduTable, KuduClient kuduClient, String _idValue, BsonDocument incrDoc, boolean isSrcFieldNameWTUpperCase) {
        KuduAgent agent = new KuduAgent();
        List<KuduColumn> list = new ArrayList<>();

        KuduColumn columnRK = new KuduColumn();
        columnRK.setColumnName(Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME).setColumnType(Type.STRING).setSelect(true).setComparisonOp(KuduPredicate.ComparisonOp.EQUAL).setComparisonValue(_idValue);
        list.add(columnRK);

        Map<String, Type> fieldName2TypeMap = new HashMap<>();
        for(String fieldName: incrDoc.keySet()) {
            KuduColumn column2Query = new KuduColumn();
            Type type = KuduType.getColumnType(kuduTable, fieldName);
            fieldName2TypeMap.put(fieldName, type);
            column2Query.setColumnName(fieldName).setSelect(true).setColumnType(type);

            list.add(column2Query);
        }
        List<Map<String, Object>> selected = agent.select(kuduTable.getName(), kuduClient, list);

        Operation ret;
        if(selected.size() == 0) {
            ret = getOperation(OperationType.CREATE, kuduTable, incrDoc, isSrcFieldNameWTUpperCase);
        } else {
            Map<String, Object> retMap = selected.get(0);
            ret = kuduTable.newUpdate();
            PartialRow row = ret.getRow();
            row.addObject(Constants.RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME, _idValue);
            for(String fieldName: incrDoc.keySet()) {
                Object incrValue = MyBson.getJavaObjFrom(fieldName, incrDoc.get(fieldName));
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
                String realKey4SinkDestination;
                if(isSrcFieldNameWTUpperCase)
                    realKey4SinkDestination = fieldName.toLowerCase();
                else
                    realKey4SinkDestination = fieldName;
                row.addObject(realKey4SinkDestination, newValue);
            }
            long currmilli = Instant.now().toEpochMilli();
            row.addObject(Constants.FIELDNAME_MODIFIED_TS, currmilli);
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
            String realKey4SinkDestination;
            if(isSrcFieldNameWTUpperCase)
                realKey4SinkDestination = key.toLowerCase();
            else
                realKey4SinkDestination = key;
            if(realKey4SinkDestination.equals("$v"))
                LOGGER.info("doc:{}, value:{}, javaValue:{}", doc, value, javaValue);
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

    static public void haveRowAddedBy(PartialRow row, Map<Integer, Object> map) {
        Set<Integer> keys = map.keySet();
        for (Integer index : keys) {
            Object value = map.get(index);
            row.addObject(index, value);
        }
    }

    static public Operation getOperation(OperationType operationType, KuduTable collection, Map<Integer, Object> valueMap, Integer dwsynctsFieldIndex) {
        Operation operation = getNewOperation(operationType, collection);
        if(operation != null) {
            PartialRow row = operation.getRow();
            if(operationType != OperationType.DELETE && dwsynctsFieldIndex != null) {
                long currmilli = Instant.now().toEpochMilli();
                row.addObject(dwsynctsFieldIndex, currmilli);
            }
            haveRowAddedBy(row, valueMap);
        }
        return operation;
    }
}
