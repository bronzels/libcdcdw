package at.bronzels.libcdcdw.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MyCollection {
    static public <T> Set<T> intersect(Set<T> set1, Set<T> set2) {
        if( set1 == null || set2 == null)
            return null;
        Set<T> retSet = new HashSet<>(set1);
        retSet.retainAll(set2);

        return retSet;
    }

    static public <T> boolean isSame(Set<T> set1, Set<T> set2) {
        if( set1 == null && set2 == null)
            return true;
        if(!MyObj.isEitherAllNullOrNotnull(set1, set2))
            return false;
        int size1 = set1.size();
        if (size1 != set2.size()) return false;
        Set<T> checkSet = intersect(set1, set2);
        return checkSet.size() == size1;
    }

    static public <T> Set<T> minus(Set<T> setBefore, Set<T> setAfter) {
        if( setBefore == null)
            return null;
        if( setAfter == null)
            return setBefore;
        Set<T> retSet = new HashSet<>(setBefore);
        retSet.removeAll(setAfter);

        return retSet;
    }

    static public <T> Map<String, T> getLowerCasedMap(Map<String, T> input) {
        return input.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue)
                );
    }

    static public Set<String> getLowerCasedSet(Set<String> input) {
        return input.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

}
