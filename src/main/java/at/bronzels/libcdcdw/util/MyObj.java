package at.bronzels.libcdcdw.util;

public class MyObj {
    static public <T> boolean isEitherAllNullOrNotnull(T... objs) {
        T first = objs[0];
        boolean nullOrNotnull = first == null;
        for(int i = 1; i < objs.length; i ++) {
            if(nullOrNotnull != (objs[i] == null))
                return false;
        }
        return true;
    }

}
