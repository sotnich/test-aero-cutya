package tinkoff.dwh.cut;

import java.util.HashSet;

// Обеединение ключей из разных таблиц, но которые по сути одно и то же
public class SingleKey {
    HashSet<Column> m_keys = new HashSet<Column>();

    public HashSet<Column> getKeys() {
        return m_keys;
    }

    public void addColumn(Column key) {
        m_keys.add(key);
    }
}
