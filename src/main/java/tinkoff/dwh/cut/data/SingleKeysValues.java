package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.SingleKey;

import java.util.*;

// Значения привязанные к SingleKey
public class SingleKeysValues {
    private HashMap<SingleKey, List<String>> m_values = new HashMap<SingleKey, List<String>>();

    public SingleKeysValues(List<SingleKey> singleKeys) {
        for (SingleKey singleKey : singleKeys)
            m_values.put(singleKey, new ArrayList<String>());
    }

    public SingleKey getSingleKey(Column column) {
        for (SingleKey singleKey : m_values.keySet()) {
            if (singleKey.contains(column))
                return singleKey;
        }
        return null;
    }

    public List<String> getValues(Column column) {
        return m_values.get(getSingleKey(column));
    }

    public ColumnValues addValues(ColumnValues values) {
        ColumnValues ret = new ColumnValues(values.getColumn());
        List<String> curValues = getValues(values.getColumn());
        if (curValues == null)
            return ret;
        for (String value : values.getValues()) {
            if (!curValues.contains(value)) {
                ret.addVaue(value);
                curValues.add(value);
            }
        }
        return ret;
    }
}
