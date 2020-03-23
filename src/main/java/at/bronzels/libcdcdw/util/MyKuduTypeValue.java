package at.bronzels.libcdcdw.util;

import at.bronzels.libcdcdw.bean.MyLogContext;
import io.vavr.Tuple2;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kudu.Type;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MyKuduTypeValue {
    static public String formatDateTimeMilli = "yyyy-MM-dd HH:mm:ss.SSS";

    static public Object getDoubleConverted(Object value, Type inputType) {
        Double typeValue = (Double) value;
        Object ret = typeValue;
        if (!inputType.equals(Type.DOUBLE)) {
            if (inputType.equals(Type.INT64))
                ret = typeValue.longValue();
            else if (inputType.equals(Type.STRING))
                ret = String.valueOf(typeValue);
            else if (inputType.equals(Type.UNIXTIME_MICROS))
                //ret = MyDateTime.timeStampLong2Date(typeValue.longValue(), formatDateTimeMilli);
                ret = typeValue.longValue();
            else if (inputType.equals(Type.BOOL))
                ret = !typeValue.equals(0.0);
            else
                ret = null;
        }
        return ret;
    }

    static public Object getLongConverted(Object value, Type inputType) {
        Long typeValue = (Long) value;
        Object ret = typeValue;
        if (!inputType.equals(Type.INT64) && !inputType.equals(Type.INT32)) {
            if (inputType.equals(Type.DOUBLE))
                ret = typeValue.doubleValue();
            else if (inputType.equals(Type.STRING))
                ret = String.valueOf(typeValue);
            else if (inputType.equals(Type.UNIXTIME_MICROS))
                //ret = MyDateTime.timeStampLong2Date(typeValue, formatDateTimeMilli);
                ret = (typeValue + 1000*60*60*8) * 1000;
            else if (inputType.equals(Type.BOOL))
                ret = !typeValue.equals(0L);
            else
                ret = null;
        }
        return ret;
    }

    static public Object getStringConverted(Object value, Type inputType) {
        String typeValue = (String) value;
        Object ret = typeValue;
        //if (!inputType.equals(Type.STRING) && !inputType.equals(Type.UNIXTIME_MICROS)) {
        if (!inputType.equals(Type.STRING)) {
            if (inputType.equals(Type.DOUBLE) && NumberUtils.isNumber(typeValue))
                ret = Double.parseDouble((String) ret);
            else if (inputType.equals(Type.INT64) && NumberUtils.isNumber(typeValue))
                ret = Long.parseLong((String) ret);
            else if (inputType.equals(Type.BOOL)) {
                String typeValueLowercased = typeValue.toLowerCase();
                if(typeValueLowercased.equals("true"))
                    ret = true;
                else if(typeValueLowercased.equals("false"))
                    ret = false;
                else
                    ret = null;
            }
            else if (inputType.equals(Type.UNIXTIME_MICROS))
                ret = MyDateTime.date2TimeStampLong(typeValue, formatDateTimeMilli);
            else
                ret = null;
        }
        return ret;
    }

    static public Object getBoolConverted(Object value, Type inputType) {
        Boolean boolValue = (Boolean) value;
        Object ret = boolValue;
        if (!inputType.equals(Type.BOOL)) {
            if (inputType.equals(Type.DOUBLE))
                ret = boolValue ? 1.0 : 0.0;
            else if (inputType.equals(Type.INT64))
                ret = boolValue ? 1L : 0L;
            else if (inputType.equals(Type.STRING))
                ret = boolValue ? "true" : "false";
        }
        return ret;
    }

    static public io.vavr.Tuple2<Type, Object> getTypeValueByBsonValue(BsonValue value) {
        Type kuduType = null;
        Object kuduValue = null;
        if (value.isDouble()) {
            kuduType = Type.DOUBLE;
            kuduValue = value.asDouble().getValue();
        } else if (value.isInt64()) {
            kuduType = Type.INT64;
            kuduValue = value.asInt64().getValue();
        } else if (value.isInt32()) {
            kuduType = Type.INT64;
            kuduValue = Integer.valueOf(value.asInt32().getValue()).longValue();
        } else if (value.isString()) {
            kuduType = Type.STRING;
            kuduValue = value.asString().getValue();
        } else if (value.isBoolean()) {
            kuduType = Type.BOOL;
            kuduValue = value.asBoolean().getValue();
        } else if( value.isTimestamp()) {
            kuduType = Type.UNIXTIME_MICROS;
            kuduValue = value.asTimestamp().getValue();
        } else if (value.isArray() || value.isDocument()) {
            kuduType = Type.STRING;
            kuduValue = value.toString();
        }

        if (kuduType != null && kuduValue != null)
            return new io.vavr.Tuple2<Type, Object>(kuduType, kuduValue);
        else
            return null;
    }

    static public Object getValueByBsonValueType(BsonValue value, Type inputType) throws NumberFormatException {
        Object ret = null;
        try {
            if (value.isDouble()) {
                ret = value.asDouble().getValue();
                ret = getDoubleConverted(ret, inputType);
            } else if (value.isInt64() || value.isInt32()) {
                if (value.isInt64()) {
                    ret = value.asInt64().getValue();
                } else {
                    ret = Integer.valueOf(value.asInt32().getValue()).longValue();
                }
                ret = getLongConverted(ret, inputType);
            } else if (value.isString()) {
                ret = value.asString().getValue();
                ret = getStringConverted(ret, inputType);
            } else if (value.isBoolean()) {
                ret = value.asBoolean().getValue();
                ret = getBoolConverted(ret, inputType);
            } else if (value.isTimestamp()) {
                ret = value.asTimestamp().getValue();
                ret = getLongConverted(ret, inputType);
            } else if (value.isArray() || value.isDocument()) {
                ret = value.toString();
                if (!inputType.equals(Type.STRING))
                    ret = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    static public Tuple2<Map<String, Type>, Map<String, Object>> getCol2AddAndValueMapTuple(Map<String, Type> colName2TypeMap, BsonDocument doc, boolean isSrcFieldNameWTUpperCase, MyLogContext logContext) {
        Map<String, Type> col2AddMap = getBsonCol2Add(colName2TypeMap, doc, isSrcFieldNameWTUpperCase);
        Map<String, Object> valueMap = new HashMap<>();
        Set<String> keySet = doc.keySet();
        for (String key : keySet) {
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            Type inputType = colName2TypeMap.get(realKey4SinkDestination) == null ? col2AddMap.get(realKey4SinkDestination): colName2TypeMap.get(realKey4SinkDestination);
            Object convertedValue = null;
            try {
                convertedValue = getValueByBsonValueType(doc, inputType);
            } catch (NumberFormatException nfException) {
                nfException.printStackTrace();
            }
            if (convertedValue == null) {
                MyLogContextMsg.logNodeError(doc, logContext, "unsupported jsonNode/kuduTableFieldType detected, or parse error");
            } else {
                valueMap.put(realKey4SinkDestination, convertedValue);
            }
        }
        return new Tuple2<Map<String, Type>, Map<String, Object>>(col2AddMap, valueMap);
    }

    static public Map<String, Type> getBsonCol2Add(Map<String, Type> colName2TypeMap, BsonDocument doc, boolean isSrcFieldNameWTUpperCase) {
        Set<String> colNameSet = colName2TypeMap.keySet();
        Map<String, Type> col2AddMap = new HashMap<>();
        Set<String> keySet = doc.keySet();
        for (String key : keySet) {
            BsonValue value = doc.get(key);
            String realKey4SinkDestination = isSrcFieldNameWTUpperCase ? key.toLowerCase() : key;
            if (!colNameSet.contains(realKey4SinkDestination)) {
                io.vavr.Tuple2<Type, Object> tuple = getTypeValueByBsonValue(value);
                if (tuple != null) {
                    col2AddMap.put(realKey4SinkDestination, tuple._1);
                }
            }
        }
        return col2AddMap;
    }


}
