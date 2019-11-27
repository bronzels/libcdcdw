package at.bronzels.libcdcdw.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MyDateTime {
    static public String date2TimeStamp(String dateString, String formatString) {
        String ret = null;
        try {
            ret = String.valueOf(date2TimeStampLong(dateString, formatString));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }

    static public Long date2TimeStampLong(String dateString, String formatString) {
        Long ret = null;
        try {
            Date date = new SimpleDateFormat(formatString).parse(dateString);
            //long unixTimestamp = date.getTime();
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            long unixTimestamp = calendar.getTimeInMillis();
            ret = unixTimestamp;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }

    static public String timeStamp2Date(String timestampString, String formatString) {
        String ret = null;
        try {
            Long timestamp = Long.parseLong(timestampString);
            ret = timeStampLong2Date(timestamp, formatString);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }

    static public String timeStampLong2Date(long timestamp, String formatString) {
        String ret = null;
        try {
            ret = new SimpleDateFormat(formatString).format(new Date(timestamp));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return ret;
        }
    }


}
