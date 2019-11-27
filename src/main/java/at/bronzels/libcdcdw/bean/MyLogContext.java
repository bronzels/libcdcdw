package at.bronzels.libcdcdw.bean;

import java.io.Serializable;

public class MyLogContext implements Serializable {
    static public String FIELD_NAME_launchedMS = "launchedMS";
    private Long launchedMS;
    static public String FIELD_NAME_appName = "appName";
    private String appName;

    public MyLogContext(Long launchedMS, String appName) {
        this.launchedMS = launchedMS;
        this.appName = appName;
    }

    public Long getLaunchedMS() {
        return launchedMS;
    }

    public void setLaunchedMS(Long launchedMS) {
        this.launchedMS = launchedMS;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
