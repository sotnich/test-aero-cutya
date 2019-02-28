package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.CutLinkTable;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.SingleKey;

import java.util.*;

// Значения привязанные к SingleKey
public class SingleKeysValues {
    private HashMap<SingleKey, HashSet<String>> m_values = new HashMap<SingleKey, HashSet<String>>();
    private HashMap<SingleKey, HashSet<String>> m_missCache = new HashMap<SingleKey, HashSet<String>>();        // Кэш значений которые не нашлись в свзяках

    public SingleKeysValues(List<SingleKey> singleKeys) {
        for (SingleKey singleKey : singleKeys) {
            m_values.put(singleKey, new HashSet<String>());
            m_missCache.put(singleKey, new HashSet<String>());
        }
    }

    public SingleKey getSingleKey(Column column) {
        for (SingleKey singleKey : m_values.keySet()) {
            if (singleKey.contains(column))
                return singleKey;
        }
        return null;
    }

    public Set<String> getValues(Column column) {
        return m_values.get(getSingleKey(column));
    }

    public Set<String> getValues(SingleKey sk) {
        return m_values.get(sk);
    }

    public void addLinkTable(CutLinkTable linkTable) {
        for (SingleKey singleKey : m_values.keySet())
            singleKey.AddLinkTable(linkTable);
    }

    public Set<SingleKey> getSingleKeys() {
        return m_values.keySet();
    }

    public void putMissValues(Column column, List<String> originalValues, Set<String> foundValues) {
        SingleKey sk = getSingleKey(column);
        HashSet<String> values = new HashSet<String>(originalValues);
        values.removeAll(foundValues);
        m_missCache.get(sk).addAll(values);
    }

    public Set<String> removeAlreadyMissed(Column column, List<String> values) {
        Set<String> ret = new HashSet<String>(values);
        ret.removeAll(m_missCache.get(getSingleKey(column)));
        return ret;
    }
}
