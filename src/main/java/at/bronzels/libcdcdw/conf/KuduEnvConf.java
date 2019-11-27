package at.bronzels.libcdcdw.conf;

import java.io.Serializable;

public class KuduEnvConf implements Serializable {
    private String dbUrl;
    private String dbUser;
    private String dbPwd;
    private String dbDatabase;

    private String catalog;

    public KuduEnvConf(String dbUrl, String catalog){
        this.dbUrl = dbUrl;
        this.catalog = catalog;
    }

    public KuduEnvConf(String dbUrl, String dbUser, String dbPwd, String catalog) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPwd = dbPwd;
        this.catalog = catalog;
    }

    public KuduEnvConf(String dbUrl, String dbUser, String dbPwd, String dbDatabase, String catalog) {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPwd = dbPwd;
        this.dbDatabase = dbDatabase;
        this.catalog = catalog;
    }

    public KuduEnvConf(String dbUrl, String dbDatabase, String catalog){
        this.dbUrl = dbUrl;
        this.dbDatabase = dbDatabase;
        this.catalog = catalog;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPwd() {
        return dbPwd;
    }

    public void setDbPwd(String dbPwd) {
        this.dbPwd = dbPwd;
    }

    public String getDbDatabase() {
        return dbDatabase;
    }

    public void setDbDatabase(String dbDatabase) {
        this.dbDatabase = dbDatabase;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }
}
