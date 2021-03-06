package tinkoff.dwh.cut.meta;

import tinkoff.dwh.cut.CutLinkTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// Обеединение ключей из разных таблиц, но которые по сути одно и то же
public class SingleKey {
    private ArrayList<Column> m_keys = new ArrayList<Column>();
    private HashMap<Column, CutLinkTable> m_linkTables = new HashMap<Column, CutLinkTable>();

    public ArrayList<Column> getKeys() {
        return m_keys;
    }

    public void addColumn(Column key) {
        if (!m_keys.contains(key))
            m_keys.add(key);
    }

    public void AddLinkTable(CutLinkTable linkTable) {
        for (Column linkColumn : linkTable.getTable().getColumns())
            for (Column thisColumn : m_keys)
                if (linkColumn.equals(thisColumn))
                    m_linkTables.put(thisColumn, linkTable);
    }

    public boolean contains(Column c) {
        return m_keys.contains(c);
    }

    public List<Column> getColumns() {
        return m_keys;
    }

    @Override
    public boolean equals(Object o) {
        SingleKey ok = (SingleKey) o;
        return ok.getKeys().containsAll(m_keys) && m_keys.containsAll(ok.getKeys()) && m_keys.size() == ok.getKeys().size();
    }

    @Override
    public String toString() {
        String ret = "{";
        for (int i = 0; i < m_keys.size(); i++) {
            ret += m_keys.get(i);
            if (i < m_keys.size() - 1)
                ret += ", ";
        }
        ret += "}";
        return ret;
    }

    @Override
    public int hashCode() {
        return m_keys.hashCode();
    }
}
