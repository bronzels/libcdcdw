package at.bronzels.libcdcdw.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MyString {
    // 速度挺慢的，平均耗时15ms
    static public String concatBySkippingEmpty(String sep, List<String> strList) {
        List<String> retList = strList.stream()
                //.filter((String str) -> str != null && !"".equals(str.trim()))
                .filter((String str) -> str != null && !"".equals(str))
                //.map(String::trim)
                .collect(Collectors.toList());
        String ret = retList.isEmpty() ? "" : StringUtils.join(retList, sep);

        return ret;
    }

    static public String concatBySkippingEmpty(String sep, String... strs) {
        return concatBySkippingEmpty(sep, Arrays.asList(strs));
    }

    static public String concatBySkippingEmpty(String sep, List<String> strList, String... strs) {
        List<String> retList = new ArrayList<String>(strList);
        retList.addAll(Arrays.asList(strs));
        return concatBySkippingEmpty(sep, retList);
    }

    static public String concatBySkippingEmptyAsPrefix(String sep, String... strs) {
        String ret = concatBySkippingEmpty(sep, strs);
//        ret = ret.isEmpty()?ret:ret+sep;
        ret = ret.isEmpty()?sep:ret;
        return ret;
    }
}
