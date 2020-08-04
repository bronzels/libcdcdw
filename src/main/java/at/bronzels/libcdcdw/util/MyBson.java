package at.bronzels.libcdcdw.util;

import at.bronzels.libcdcdw.Constants;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MyBson {
    static public Object getJavaObjFrom(String key, BsonValue value) {
        Object ret;
        if (value.isNull())
            ret = null;
        else if (value.isBoolean())
            ret = value.asBoolean().getValue();
        else if (value.isDouble())
            ret = value.asDouble().getValue();
        else if (value.isInt32())
            ret = value.asInt32().getValue();
        else if (value.isInt64())
            ret = value.asInt64().getValue();
        else if (value.isString())
            ret = value.asString().getValue();
        else if (value.isTimestamp())
            ret = new Timestamp(value.asTimestamp().getValue());
        else if (value.isDateTime())
            ret = MyDateTime.timeStampLong2Date(value.asDateTime().getValue(), Constants.defaultTimestampFormatMSStr);
        else if (value.isDecimal128())
            ret = value.asDecimal128().getValue().doubleValue();
        else if (value.isObjectId())
            ret = value.asObjectId().getValue().toString();
        else
            throw new RuntimeException("error: value data type is not supported, key:" + key + ", value:" + value.toString());
        return ret;
    }

    static public BsonDocument getProjected(BsonDocument input, List<String> toProjectList) {
        Set<String> keys = input.keySet();
        BsonDocument ret = new BsonDocument();
        for(String key: keys) {
            ret.put(key, input.get(key));
        }
        return ret;
    }

    static public BsonDocument getMerged(BsonDocument... inputArr) {
        BsonDocument ret = new BsonDocument();
        for(BsonDocument doc: inputArr) {
            for(String key: doc.keySet()) {
                ret.put(key, doc.get(key));
            }
        }
        return ret;
    }

    static public Map<String, Object> getMap(BsonDocument doc) {
        Map<String, Object> ret = new HashMap<>();
        for(String key: doc.keySet()) {
            ret.put(key, getJavaObjFrom(key, doc.get(key)));
        }
        return ret;
    }
}
