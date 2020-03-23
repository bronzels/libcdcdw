package at.bronzels.libcdcdw.util;

import at.bronzels.libcdcdw.bean.MyLogContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ObjectMessage;

import java.util.HashMap;
import java.util.Map;

public class MyLogContextMsg {

    private static org.apache.logging.log4j.Logger lo4j2LOG = LogManager.getLogger(MyLogContextMsg.class);

    static public void logNodeError(Object node, MyLogContext logContext, String errPrompt) {
        if(lo4j2LOG.isErrorEnabled()) {
            Map<String, Object> map = new HashMap<>();
            map.put("node", node.toString());
            MyLog4j2.markBfLog(logContext, errPrompt);
            ObjectMessage msg = new ObjectMessage(map);
            lo4j2LOG.warn(msg);
            MyLog4j2.unmarkAfLog();
        }
    }


}
