package at.bronzels.libcdcdw.myenum;

public enum AppModeEnum {
    LOCAL("local"),
    REMOTE("remote"),
    SUBMIT("submit");

    private String name;

    AppModeEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    static public AppModeEnum fromName(String name) {
        for (AppModeEnum mode : AppModeEnum.values()) {
            if (mode.getName().equals(name)) {
                return mode;
            }
        }
        return null;
    }

}
