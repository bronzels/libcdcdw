package at.bronzels.libcdcdw.util;

import at.bronzels.libcdcdw.bean.MyLogContext;
import org.apache.logging.log4j.ThreadContext;

public class MyLog4j2 {
    static public String MARKER_NAME_COMMONAPP_launchedms = "launchedms";
    static public String MARKER_NAME_COMMONAPP_appname = "appname";

    static private String MARKER_NAME_title = "title";
    static private String MARKER_NAME_method = "method";
    static private String MARKER_NAME_line = "line";

    static private String MARKER_NAME_ctmilli = "ctmilli";

    static void markCommonApp(MyLogContext myLogContext) {
        ThreadContext.put(MARKER_NAME_COMMONAPP_launchedms, myLogContext != null ? String.valueOf(myLogContext.getLaunchedMS()) : null);
        ThreadContext.put(MARKER_NAME_COMMONAPP_appname, myLogContext != null ? myLogContext.getAppName() : null);
        ThreadContext.put(MARKER_NAME_ctmilli, String.valueOf(System.currentTimeMillis()));
    }

    static public void unmarkCommonApp() {
        ThreadContext.remove(MARKER_NAME_COMMONAPP_launchedms);
        ThreadContext.remove(MARKER_NAME_COMMONAPP_appname);
        ThreadContext.remove(MARKER_NAME_ctmilli);
    }

    static public void markBfLog(MyLogContext myLogContext, String title) {
        markCommonApp(myLogContext);
        StackTraceElement ste = Thread.currentThread().getStackTrace()[2];
        ThreadContext.put(MARKER_NAME_title, title);
        ThreadContext.put(MARKER_NAME_method, ste.getMethodName());
        ThreadContext.put(MARKER_NAME_line, String.valueOf(ste.getLineNumber()));
    }

    static public void unmarkAfLog() {
        ThreadContext.remove(MARKER_NAME_title);
        ThreadContext.remove(MARKER_NAME_method);
        ThreadContext.remove(MARKER_NAME_line);
        unmarkCommonApp();
    }

    static public void markTitleBfLog(MyLogContext myLogContext, String title) {
        markCommonApp(myLogContext);
        ThreadContext.put(MARKER_NAME_title, title);
    }

    static public void unmarkTitleAfLog() {
        ThreadContext.remove(MARKER_NAME_title);
        unmarkCommonApp();
    }

    /*
    static public void markBfLog(Map<String, Object> map, String title, String method, long line) {
        map.put(MARKER_NAME_title, title);
        map.put(MARKER_NAME_method, method);
        map.put(MARKER_NAME_line, line);
    }
     */
}
