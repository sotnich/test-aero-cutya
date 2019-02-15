package tinkoff.dwh.cut.meta;

import java.util.ArrayList;

public class Table {
    private String m_tableName;
    private ArrayList<Column> m_columns = new ArrayList<Column>();

    public Table(String tableName) {
        m_tableName = tableName;
    }

    public Table(String tableName, ArrayList<Column> columns) {
        m_tableName = tableName;
        for (Column column : columns)
            addColumn(column);
    }

    public void addColumn(Column column) {
        if (!m_columns.contains(column))
            m_columns.add(column);
    }

    public ArrayList<Column> getColumns() {
        return m_columns;
    }

    public String getTableName() {
        return m_tableName;
    }

    @Override
    public boolean equals(Object o) {
        Table t = (Table) o;
        return m_tableName.equals(t.m_tableName) && m_columns.size() == t.m_columns.size() && m_columns.containsAll(t.m_columns) && t.m_columns.containsAll(m_columns);
    }

    @Override
    public String toString() {
        String ret = "{";
        for (int i = 0; i < m_columns.size(); i++) {
            ret += m_columns.get(i);
            if (i < m_columns.size() - 1)
                ret += ", ";
        }
        ret += "}";
        return ret;
    }
}
