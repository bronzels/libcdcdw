package at.bronzels.libcdcdw.kudu.pool;

import at.bronzels.libcdcdw.OperationType;
import at.bronzels.libcdcdw.kudu.KuduOperation;
import org.apache.kudu.client.Operation;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyBsonKudu extends MyKudu{
    private final Logger LOG = LoggerFactory.getLogger(MyBsonKudu.class);

    public MyBsonKudu(String catalog, String kuduUrl, String kuduDatabase, String tableName) {
        super(catalog, kuduUrl, kuduDatabase, tableName);
    }

    public void put(BsonDocument valueDoc, boolean isSrcFieldNameWTUpperCase) {
        Operation op = KuduOperation.getOperation(OperationType.UPDATE, kuduTable, valueDoc, isSrcFieldNameWTUpperCase);
        applyOp(op);
    }

    public void delete(BsonDocument valueDoc, boolean isSrcFieldNameWTUpperCase) {
        Operation op = KuduOperation.getOperation(OperationType.DELETE, kuduTable, valueDoc, isSrcFieldNameWTUpperCase);
        applyOp(op);
    }

}
