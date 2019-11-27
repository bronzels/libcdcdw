package at.bronzels.libcdcdw.kudu;

import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;

public class KuduType {
    static public Type getColumnType(KuduTable kuduTable, String fieldName) {
        return kuduTable.getSchema().getColumn(fieldName).getType();
    }
}
