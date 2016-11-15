package defaultlogs.pojo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by synerzip on 10/11/16.
 */
public class DefaultLogs {
    public static Map<String,Log> logTypes = new HashMap<String,Log>();
    static {
        logTypes.put("combined",new CombinedLog());
        logTypes.put("common",new CommonLog());
        logTypes.put("sys",new SysLog());
    }
}
