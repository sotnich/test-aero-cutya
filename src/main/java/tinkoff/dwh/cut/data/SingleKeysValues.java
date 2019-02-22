package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.SingleKey;

import java.util.*;

// Значения привязанные к SingleKey
public class SingleKeysValues {
    private HashMap<SingleKey, HashSet<String>> m_values = new HashMap<SingleKey, HashSet<String>>();

    public SingleKeysValues(List<SingleKey> singleKeys) {
        for (SingleKey singleKey : singleKeys)
            m_values.put(singleKey, new HashSet<String>());
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

    public void addValuesAndRemoveExisting(ColumnValues values, boolean add_flg) {
        Set<String> curValues = getValues(values.getColumn());
        values.getValues().removeAll(curValues);                    // Удаляем в новых те, которые уже есть
        if (add_flg)
            curValues.addAll(values.getValues());
    }
}
