package at.bronzels.libcdcdw.util;

import java.io.IOException;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MagicDateTime {

    static String MONTH = "";
    static String dateRegEx = "";
    static Pattern DatePattern;
    static HashMap<String, Integer> monthMap = new HashMap<String, Integer>();

    static {
        initializeMonthName();
        dateRegEx = "(?i)(\\d{1,4}|" + MONTH + ")[-|/|.|\\s+]?(\\d{1,2}|" + MONTH + ")[-|/|.|,]?[\\s+]?(\\d{1,4}|" + MONTH + ")[\\s+|\\t|T]?(\\d{0,2}):?(\\d{0,2}):?(\\d{0,2})[.|,]?[\\s]?(\\d{0,3})?([+|-])?(\\d{0,2})[:]?(\\d{0,2})[\\s+]?([A|P]M)?";
        DatePattern = Pattern.compile(dateRegEx);
    }

    private static void initializeMonthName() {
        String[] monthName = getMonthString(true);
        for (int i = 0; i < 12; i++) {
            monthMap.put(monthName[i].toLowerCase(), Integer.valueOf(i + 1));
        }

        monthName = getMonthString(false);
        for (int i = 0; i < 12; i++) {
            monthMap.put(monthName[i].toLowerCase(), Integer.valueOf(i + 1));
        }

        Iterator<String> it = monthMap.keySet().iterator();
        while (it.hasNext()) {
            String month = it.next();
            if (MONTH.isEmpty()) {
                MONTH = month;
            } else {
                MONTH = MONTH + "|" + month;
            }
        }
    }

    private static boolean isInteger(Object object) {
        if (object instanceof Integer) {
            return true;
        } else {
            try {
                Integer.parseInt(object.toString());
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    private static String[] getMonthString(boolean isShort) {
        DateFormatSymbols dfs = new DateFormatSymbols();
        if (isShort) {
            return dfs.getShortMonths();
        } else {
            return dfs.getMonths();
        }
    }

    private static int getMonthMap(String value) {

        if (monthMap.get(value) == null) {
            return 0;
        }
        return monthMap.get(value).intValue();
    }

    private static long getMilliStackoverflow(String value) {

        Matcher matcher = DatePattern.matcher(value);
        int Year = 0, Month = 0, Day = 0;
        boolean isYearFound = false;
        boolean isMonthFound = false;
        boolean isDayFound = false;
        if (matcher.find()) {
            for (int i = 1; i < matcher.groupCount(); i++) {
                String data = matcher.group(i) == null ? "" : matcher.group(i);
                if (data.equalsIgnoreCase("null")) {
                    data = "";
                }
                //System.out.println(String.valueOf(i) + ": " + data);
                switch (i) {
                    case 1:
                        if (!data.isEmpty()) {
                            if (isInteger(data)) {
                                Integer YMD = Integer.valueOf(data);
                                if (YMD == 0) {
                                    return 0;
                                }
                                if (YMD > 31) {
                                    Year = YMD.intValue();
                                    isYearFound = true;
                                } else if (YMD > 12) {
                                    Day = YMD.intValue();
                                    isDayFound = true;
                                } else {
                                    Month = YMD.intValue();
                                    isMonthFound = true;
                                }
                            } else {
                                Month = getMonthMap(data.toLowerCase());
                                if (Month == 0) {
                                    return 0;
                                }
                                isMonthFound = true;
                            }
                        } else {
                            return 0;
                        }
                        break;
                    case 2:
                        if (!data.isEmpty()) {
                            if (isInteger(data)) {
                                Integer YMD = Integer.valueOf(data);
                                if (YMD == 0) {
                                    return 0;
                                }

                                if (YMD > 31) {
                                    if (isYearFound) {
                                        return 0;
                                    }
                                    Year = YMD.intValue();
                                    isYearFound = true;
                                } else if (YMD > 12) {
                                    if (isDayFound) {
                                        return 0;
                                    }
                                    Day = YMD.intValue();
                                    isDayFound = true;
                                } else {
                                    if (isMonthFound) {
                                        Day = YMD.intValue();
                                        isDayFound = true;
                                    } else {
                                        Month = YMD.intValue();
                                        isMonthFound = true;
                                    }
                                }
                            } else {
                                if (isMonthFound) {
                                    Day = Month;
                                    isDayFound = true;
                                }
                                Month = getMonthMap(data.toLowerCase());
                                if (Month == 0) {
                                    return 0;
                                }

                                isMonthFound = true;
                            }
                        } else {
                            return 0;
                        }
                        break;
                    case 3:
                        if (!data.isEmpty()) {
                            if (isInteger(data)) {

                                Integer YMD = Integer.valueOf(data);
                                if (YMD == 0) {
                                    return 0;
                                }
                                if (YMD > 31) {
                                    if (isYearFound) {
                                        return 0;
                                    }
                                    Year = YMD.intValue();
                                    isYearFound = true;
                                } else if (YMD > 12) {
                                    if (isDayFound) {
                                        return 0;
                                    }
                                    Day = YMD.intValue();
                                    isDayFound = true;
                                } else {
                                    if (isMonthFound) {
                                        Day = YMD.intValue();
                                        isDayFound = true;
                                    } else {
                                        Month = YMD.intValue();
                                        isMonthFound = true;
                                    }

                                }
                            } else {
                                if (isMonthFound) {
                                    Day = Month;
                                    isDayFound = true;
                                }
                                Month = getMonthMap(data.toLowerCase());
                                if (Month == 0) {
                                    return 0;
                                }
                                isMonthFound = true;
                            }
                        } else {
                            return 0;
                        }
                        break;
                    case 4:
                        //hour
                        break;
                    case 5:
                        //minutes
                        break;
                    case 6:
                        //second
                        break;
                    case 7:
                        //millisecond
                        break;
                    case 8:
                        //time zone +/-
                        break;
                    case 9:
                        //time zone hour
                        break;
                    case 10:
                        // time zone minute
                        break;
                    case 11:
                        //AM/PM
                        break;
                }

            }
        }

        Calendar c = Calendar.getInstance();
        c.set(Year, Month - 1, Day, 0, 0);
        return c.getTime().getTime();
    }

    /**
     * 常规自动日期格式识别
     *
     * @param str 时间字符串
     * @return Date
     * @author dc
     */
    private static String getDateFmtCSDN(String str) {
        boolean year = false;
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        if (pattern.matcher(str.substring(0, 4)).matches()) {
            year = true;
        }
        StringBuilder sb = new StringBuilder();
        int index = 0;
        if (!year) {
            if (str.contains("月") || str.contains("-") || str.contains("/")) {
                if (Character.isDigit(str.charAt(0))) {
                    index = 1;
                }
            } else {
                index = 3;
            }
        }
        for (int i = 0; i < str.length(); i++) {
            char chr = str.charAt(i);
            if (Character.isDigit(chr)) {
                if (index == 0) {
                    sb.append("y");
                }
                if (index == 1) {
                    sb.append("M");
                }
                if (index == 2) {
                    sb.append("d");
                }
                if (index == 3) {
                    sb.append("H");
                }
                if (index == 4) {
                    sb.append("m");
                }
                if (index == 5) {
                    sb.append("s");
                }
                if (index == 6) {
                    sb.append("S");
                }
            } else {
                if (i > 0) {
                    char lastChar = str.charAt(i - 1);
                    if (Character.isDigit(lastChar)) {
                        index++;
                    }
                }
                sb.append(chr);
            }
        }
        return sb.toString();
    }

    private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {{
        put("^\\d{8}$", "yyyyMMdd");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
        put("^\\d{12}$", "yyyyMMddHHmm");
        put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
        put("^\\d{14}$", "yyyyMMddHHmmss");
        put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "MM/dd/yyyy HH:mm:ss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");
    }};

    /**
     * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
     * format is unknown. You can simply extend DateUtil with more formats if needed.
     *
     * @param dateString The date string to determine the SimpleDateFormat pattern for.
     * @return The matching SimpleDateFormat pattern, or null if format is unknown.
     * @see SimpleDateFormat
     */
    private static String getDateFmtStackoverflow(String dateString) {
        for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
            if (dateString.toLowerCase().matches(regexp)) {
                return DATE_FORMAT_REGEXPS.get(regexp);
            }
        }
        return null; // Unknown format.
    }

    /*
    private static Date getDateNatty(String dateString) {
        import com.joestelmach.natty.DateGroup;
        import com.joestelmach.natty.ParseLocation;
        import com.joestelmach.natty.Parser;

        Date ret = null;
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse("the day before next thursday");
        for (DateGroup group : groups) {
            List<Date> dates = group.getDates();
            if (dates.size() > 0) {
                ret = dates.get(0);
                break;
            }
            int line = group.getLine();
            int column = group.getPosition();
            String matchingValue = group.getText();
            String syntaxTree = group.getSyntaxTree().toStringTree();
            Map<String, List<ParseLocation>> parseMap = group.getParseLocations();
            boolean isRecurreing = group.isRecurring();
            Date recursUntil = group.getRecursUntil();
        }
        return ret;
    }
     */

    public static String getDateFmtDetected(String str) {
        return getDateFmtCSDN(str);
    }

    public static void main(String[] argv) throws IOException {
        long d = 0;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        String[] timeInputArr = new String[]{
                "01-12",
                "2018-01",
                "2018-01-12",
                "11:22",
                "2018-01-12 11:22:33",
                "2018-01-12 11:22:33:333",
                "11时22分33秒333毫秒",
                "2018/01/12 11时22分33秒333毫秒",
                "当前2018年01月12日日 11时22分33秒333分"/*,

                "16 July 2012",
                "March 20 2012",
                "2012 March 20"
                */
        };

        for (String input : timeInputArr) {
            /*
            d = MagicDateTime.getMilliStackoverflow(input);
             */
            String fmt = getDateFmtCSDN(input);
            //String fmt = getDateFmtStackoverflow(input);

            d = MyDateTime.date2TimeStampLong(input, fmt);

            Date dt = new Date(d);

            //Date dt = getDateNatty(input);
            String dateText = df.format(dt);
            System.out.println("input:" + input + ", dateText:" + dateText);
        }
    }

}