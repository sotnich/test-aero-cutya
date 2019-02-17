package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.meta.Column;

import java.util.ArrayList;

public class KeyValue {
    private Column m_key;
    private String m_value;

    public KeyValue(Column key, String value) {
        m_key = key;
        m_value = value;
    }

    public Column getKey() {
        return m_key;
    }

    public String getValue() {
        return m_value;
    }

    public boolean isNonEmpty() {
        return m_value != null && m_value.length() > 0;
    }

    public static ArrayList<KeyValue> copyNonEmptyWithout(ArrayList<KeyValue> kvs, KeyValue withoutKV) {
        ArrayList<KeyValue> ret = new ArrayList<KeyValue>();
        for (KeyValue kv : kvs) {
            if (!kv.equals(withoutKV) && kv.isNonEmpty())
                ret.add(kv);
        }
        return ret;
    }

    public static ArrayList<KeyValue> fromArray(ArrayList<Column> columns, String ... values) {
        ArrayList<KeyValue> res = new ArrayList<KeyValue>();
        for (int i = 0; i < columns.size(); i++)
            res.add(new KeyValue(columns.get(i), values[i]));
        return res;
    }
}
