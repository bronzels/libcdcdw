package at.bronzels.libcdcdw;

public interface Constants {
    String commonSep = "_";

    String redisKeySep = ":";
    String slashSep = "/";
    //String distLockKeyPrefix = "bd:kv:lock";
    String distLockKeyPrefix = "bd_kv_lock";

    String FIELDNAME_MODIFIED_TS = "_dwsyncts";
    Integer tsFieldIndexReusedDwsync = -1;

    //public static final String KUDU_TABLE_NAME_PREFIX = "impala::";
    String KUDU_TABLE_NAME_AFTER_CATALOG_SEP = "::";
    String KUDU_TABLE_NAME_SEP = ".";

    String RK_4_MONGODB_AND_OTHER_DBS_ID_FIELD_NAME = "_id";

    String defaultTimestampFormatStr = "yyyy-MM-dd HH:mm:ss";
    String defaultTimestampFormatMSStr = "yyyy-MM-dd HH:mm:ss.SSS";

    Long msPrcOffset = 8 * 60 * 60 * 1000L;
    Long msPerDay = 24 * 60 * 60 * 1000L;

}
