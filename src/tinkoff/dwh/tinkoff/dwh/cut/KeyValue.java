package tinkoff.dwh.cut;

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

    public static ArrayList<KeyValue> copyWithout(ArrayList<KeyValue> kvs, KeyValue withoutKV) {
        ArrayList<KeyValue> ret = new ArrayList<KeyValue>(kvs);
        ret.remove(withoutKV);
        return ret;
    }
}
