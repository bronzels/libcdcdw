package at.bronzels.libcdcdw.kudu;

import org.apache.kudu.Type;

public class KuduValue {
    static public Object getObjIncred(Type type, Object oldValue, Object value2Incr) {
        Object ret = null;
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
                ret = (Integer)oldValue + (Integer)value2Incr;
                break;
            case INT64:
                ret = (Long)oldValue + (Long)value2Incr;
                break;
            case FLOAT:
                ret = (Float)oldValue + (Float)value2Incr;
                break;
            case DOUBLE:
                ret = (Double)oldValue + (Double)value2Incr;
                break;
        }
        return ret;
    }

}
