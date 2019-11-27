package at.bronzels.libcdcdw.conf;

public class KuduTableEnvConf extends KuduEnvConf{
    private String tblName;

    public KuduTableEnvConf(String catalog, String dbUrl, String dbUser, String dbPwd, String tblName){
        super(dbUrl, dbUser, dbPwd, catalog);
        this.tblName = tblName;
    }

    public KuduTableEnvConf(String catalog, String dbUrl, String dbUser, String dbPwd, String dbDatabase, String tblName){
        super(dbUrl, dbUser, dbPwd, dbDatabase, catalog);
        this.tblName = tblName;
    }

    public KuduTableEnvConf(String catalog, String dbUrl, String dbDatabase, String tblName){
        super(dbUrl, dbDatabase, catalog);
        this.tblName = tblName;
    }

    public KuduTableEnvConf(KuduEnvConf kuduEnvConf, String tblName){
        super(kuduEnvConf.getDbUrl(), kuduEnvConf.getDbUser(), kuduEnvConf.getDbPwd(), kuduEnvConf.getDbDatabase(), kuduEnvConf.getCatalog());
        this.tblName = tblName;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }
}
