package at.bronzels.libcdcdw.conf;

import at.bronzels.libcdcdw.Constants;
import at.bronzels.libcdcdw.util.MyString;

import java.io.Serializable;

public class DistLockConf implements Serializable {
    private String url;
    private String prefix;

    public DistLockConf(String url, String prefix) {
        this.url = url;
        this.prefix = MyString.concatBySkippingEmpty(Constants.commonSep, Constants.distLockKeyPrefix, prefix);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
