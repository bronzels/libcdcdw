package at.bronzels.libcdcdw.util;

public class Here {
    static public String at () {
        StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
        String where = "[" + ste.getClassName() + ":" + ste.getMethodName() + ":" + ste.getLineNumber() + "]";
        return where;
    }

    static public StackTraceElement getSTE() {
        return Thread.currentThread().getStackTrace()[2];

    }
}