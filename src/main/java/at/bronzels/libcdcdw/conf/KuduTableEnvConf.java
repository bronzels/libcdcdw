package at.bronzels.libcdcdw.conf;

import at.bronzels.libcdcdw.KafkaProducerWNFixedTopic210;

public class KuduTableEnvConf extends KuduEnvConf{
    private String tblName;
    private KafkaProducerWNFixedTopic210 doubleWriteProducer;


    public KuduTableEnvConf(String catalog, String dbUrl, String dbUser, String dbPwd, String tblName, KafkaProducerWNFixedTopic210 producer){
        super(dbUrl, dbUser, dbPwd, catalog);
        this.tblName = tblName;
        this.doubleWriteProducer = producer;
    }

    public KuduTableEnvConf(String catalog, String dbUrl, String dbUser, String dbPwd, String dbDatabase, String tblName, KafkaProducerWNFixedTopic210 producer){
        super(dbUrl, dbUser, dbPwd, dbDatabase, catalog);
        this.tblName = tblName;
        this.doubleWriteProducer = producer;
    }

    public KuduTableEnvConf(String catalog, String dbUrl, String dbDatabase, String tblName, KafkaProducerWNFixedTopic210 producer){
        super(dbUrl, dbDatabase, catalog);
        this.tblName = tblName;
        this.doubleWriteProducer = producer;
    }

    public KuduTableEnvConf(KuduEnvConf kuduEnvConf, String tblName, KafkaProducerWNFixedTopic210 producer){
        super(kuduEnvConf.getDbUrl(), kuduEnvConf.getDbUser(), kuduEnvConf.getDbPwd(), kuduEnvConf.getDbDatabase(), kuduEnvConf.getCatalog());
        this.tblName = tblName;
        this.doubleWriteProducer = producer;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }

    public KafkaProducerWNFixedTopic210 getDoubleWriteProducer() {
        return doubleWriteProducer;
    }

    public void setDoubleWriteProducer(KafkaProducerWNFixedTopic210 doubleWriteProducer) {
        this.doubleWriteProducer = doubleWriteProducer;
    }
}
