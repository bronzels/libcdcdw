package at.bronzels.libcdcdw.kudu.tool;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KuduTest {
    private static KuduClient client = null;

    public static void main(String[] args) throws KuduException {
//        insertSingleTEST();
//        insertMultTEST();
//        updateMultTEST();
//        updateSingleTEST();
//        deleteMultTEST();
//        deleteSingleTEST();
//        renameTEST();
//        alterColumnTEST();
        client = new KuduClient.KuduClientBuilder("beta-hbase01:7051").defaultAdminOperationTimeoutMs(600000).build();
        //selectTest();
        //kuduCreateTableTest();
        insertSingleTEST1();
        //client.close();
    }

    public static void kuduCreateTableTest(){
        try {
            List<ColumnSchema> columns = new ArrayList(2);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.UNIXTIME_MICROS)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("key");
            Schema schema = new Schema(columns);
            client.createTable("presto::jsd.kudu_micros_test", schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys));
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertSingleTEST1() throws KuduException {
        KuduTable table = client.openTable("presto::jsd.kudu_micros_test");
        KuduSession session = client.newSession();
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString(0, 3+"");
        //row.addObject(0, "2019-09-27 23:55:00.11");
        row.addTimestamp(1, new Timestamp(0L));
        OperationResponse operationResponse =  session.apply(insert);
    }

    public static void selectTest() {
        KuduAgent agent = new KuduAgent();
        KuduColumn column01 = new KuduColumn();
        column01.setColumnName("name").setColumnType(Type.STRING).setSelect(true).setComparisonOp(KuduPredicate.ComparisonOp.EQUAL).setComparisonValue("lijie001");
        KuduColumn column02 = new KuduColumn();
        column02.setColumnName("id").setSelect(true).setColumnType(Type.INT64);
        KuduColumn column03 = new KuduColumn();
        column03.setColumnName("sex").setSelect(true).setColumnType(Type.STRING);
        List<KuduColumn> list = new ArrayList<>();
        list.add(column01);
        list.add(column02);
        list.add(column03);
        List<Map<String, Object>> select = agent.select("impala::impala_kudu.my_first_table", client, list);
        System.out.println("-----------------" + select);
    }

    public static void alterColumnTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        KuduRow myrows01 = new KuduRow();
        myrows01.setTableName("impala::impala_kudu.my_first_table");
        KuduColumn c01 = new KuduColumn();
        c01.setColumnName("newsex").setNewColumnName("sex");
        c01.setAlterColumnEnum(KuduColumn.AlterColumnEnum.RENAME_COLUMN);
        KuduColumn c02 = new KuduColumn();
        c02.setColumnName("myadd").setAlterColumnEnum(KuduColumn.AlterColumnEnum.DROP_COLUMN);
        List<KuduColumn> rows01 = new ArrayList<>();
        rows01.add(c01);
        rows01.add(c02);
        myrows01.setRows(rows01);

        KuduRow myrows11 = new KuduRow();
        myrows11.setTableName("impala::impala_kudu.my_first_table");
        KuduColumn c11 = new KuduColumn();
        c11.setColumnName("newname").setNewColumnName("name");
        c11.setAlterColumnEnum(KuduColumn.AlterColumnEnum.RENAME_COLUMN);
        KuduColumn c12 = new KuduColumn();
        c12.setColumnName("nickName").setAlterColumnEnum(KuduColumn.AlterColumnEnum.ADD_COLUMN).setNullAble(false).setColumnType(Type.STRING).setDefaultValue("aaa");
        List<KuduColumn> rows11 = new ArrayList<>();
        rows11.add(c11);
        rows11.add(c12);
        myrows11.setRows(rows11);

        List<KuduRow> list = new ArrayList<>();
        list.add(myrows01);
        list.add(myrows11);

        agent.alter(client, list);
    }

    public static void renameTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        KuduRow myrows01 = new KuduRow();
        myrows01.setTableName("impala::impala_kudu.my_first_table");
        myrows01.setNewTableName("impala::impala_kudu.my_first_table1");
        myrows01.setAlterTableEnum(KuduRow.AlterTableEnum.RENAME_TABLE);

        KuduRow myrows02 = new KuduRow();
        myrows02.setTableName("impala::impala_kudu.my_first_table1");
        myrows02.setNewTableName("impala::impala_kudu.my_first_table");
        myrows02.setAlterTableEnum(KuduRow.AlterTableEnum.RENAME_TABLE);

        List<KuduRow> list = new ArrayList<>();
        list.add(myrows01);
        list.add(myrows02);

        agent.alter(client, list);
    }

    public static void deleteMultTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        //第一行
        KuduColumn c01 = new KuduColumn();
        c01.setColumnName("id").setColumnValue(1000001).setColumnType(Type.INT64).setUpdate(false).setPrimaryKey(true);
        KuduColumn c02 = new KuduColumn();
        c02.setColumnName("name").setColumnValue("lijie123").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row01 = new ArrayList<>();
        row01.add(c01);
//        row01.add(c02);
        KuduRow myrows01 = new KuduRow();
        myrows01.setRows(row01);

        //第一行
        KuduColumn c11 = new KuduColumn();
        c11.setColumnName("id").setColumnValue(1000002).setColumnType(Type.INT64).setUpdate(false).setPrimaryKey(true);
        KuduColumn c12 = new KuduColumn();
        c12.setColumnName("name").setColumnValue("lijie123").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row11 = new ArrayList<>();
        row11.add(c11);
//        row11.add(c12);
        KuduRow myrows11 = new KuduRow();
        myrows11.setRows(row11);

        List<KuduRow> rows = new ArrayList<>();
        rows.add(myrows01);
        rows.add(myrows11);
        agent.delete("impala::impala_kudu.my_first_table", client, rows);
    }

    public static void deleteSingleTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        //第一行
        KuduColumn c01 = new KuduColumn();
        c01.setColumnName("id").setColumnValue(1000003).setColumnType(Type.INT64).setUpdate(false).setPrimaryKey(true);
        KuduColumn c02 = new KuduColumn();
        c02.setColumnName("name").setColumnValue("lijie789").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row01 = new ArrayList<>();
        row01.add(c01);
//        row01.add(c02);
        KuduRow myrows01 = new KuduRow();
        myrows01.setRows(row01);
        agent.delete("impala::impala_kudu.my_first_table", client, myrows01);
    }

    public static void updateMultTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        //第一行
        KuduColumn c01 = new KuduColumn();
        c01.setColumnName("id").setColumnValue(1000001).setColumnType(Type.INT64).setUpdate(false).setPrimaryKey(true);
        KuduColumn c02 = new KuduColumn();
        c02.setColumnName("name").setColumnValue("lijie123").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row01 = new ArrayList<>();
        row01.add(c01);
        row01.add(c02);
        KuduRow myrows01 = new KuduRow();
        myrows01.setRows(row01);
        //第二行
        KuduColumn c11 = new KuduColumn();
        c11.setColumnName("id").setColumnValue(1000002).setColumnType(Type.INT64).setUpdate(false).setPrimaryKey(true);
        KuduColumn c12 = new KuduColumn();
        c12.setColumnName("name").setColumnValue("lijie456").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row11 = new ArrayList<>();
        row11.add(c11);
        row11.add(c12);
        KuduRow myrows11 = new KuduRow();
        myrows11.setRows(row11);

        List<KuduRow> rows = new ArrayList<>();
        rows.add(myrows01);
        rows.add(myrows11);
        agent.update("impala::impala_kudu.my_first_table", client, rows);
    }

    public static void updateSingleTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        //第一行
        KuduColumn c01 = new KuduColumn();
        c01.setColumnName("id").setColumnValue(12).setColumnType(Type.INT64).setUpdate(false).setPrimaryKey(true);
        KuduColumn c02 = new KuduColumn();
        c02.setColumnName("name").setColumnValue("lijie789").setColumnType(Type.STRING).setUpdate(false);
        KuduColumn c03 = new KuduColumn();
        c03.setColumnName("sex").setColumnValue("lijie789").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row01 = new ArrayList<>();
        row01.add(c01);
        row01.add(c02);
        row01.add(c03);
        KuduRow myrows01 = new KuduRow();
        myrows01.setRows(row01);
        agent.update("impala::impala_kudu.my_first_table", client, myrows01);
    }

    public static void insertMultTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        //第一行
        KuduColumn c01 = new KuduColumn();
        c01.setColumnName("id").setColumnValue(1000001).setColumnType(Type.INT64).setUpdate(false);
        KuduColumn c02 = new KuduColumn();
        c02.setColumnName("name").setColumnValue("lijie001").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row01 = new ArrayList<>();
        row01.add(c01);
        row01.add(c02);
        KuduRow myrows01 = new KuduRow();
        myrows01.setRows(row01);

        //第二行
        KuduColumn c11 = new KuduColumn();
        c11.setColumnName("id").setColumnValue(1000002).setColumnType(Type.INT64).setUpdate(false);
        KuduColumn c12 = new KuduColumn();
        c12.setColumnName("name").setColumnValue("lijie002").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row02 = new ArrayList<>();
        row02.add(c11);
        row02.add(c12);
        KuduRow myrows02 = new KuduRow();
        myrows02.setRows(row02);

        List<KuduRow> rows = new ArrayList<>();
        rows.add(myrows01);

        rows.add(myrows02);

        agent.insert("impala::impala_kudu.my_first_table", client, rows);
    }

    public static void insertSingleTEST() throws KuduException {
        KuduAgent agent = new KuduAgent();
        //第一行
        KuduColumn c01 = new KuduColumn();

        c01.setColumnName("id").setColumnValue(1000003).setColumnType(Type.INT64).setUpdate(false);
        KuduColumn c02 = new KuduColumn();
        c02.setColumnName("name").setColumnValue("lijie003").setColumnType(Type.STRING).setUpdate(false);
        List<KuduColumn> row01 = new ArrayList<>();
        row01.add(c01);
        row01.add(c02);
        KuduRow myrows01 = new KuduRow();
        myrows01.setRows(row01);
        agent.insert("impala::impala_kudu.my_first_table", client, myrows01);
    }
}
