package tinkoff.dwh.cut.meta;

import java.util.ArrayList;
import java.util.List;

public class Table {
    private String m_tableName;
    private ArrayList<Column> m_columns = new ArrayList<Column>();

    public Table(String tableName) {
        m_tableName = tableName;
    }

    public Table(Table table) {
        m_tableName = table.m_tableName;
        addColumns(table.getColumns());
    }

    public Table(String tableName, List<String> columnNames) {
        m_tableName = tableName;
        for (String columnName : columnNames)
            addColumn(new Column(tableName, columnName));
    }

    public Table(String tableName, String ... columnNames) {
        m_tableName = tableName;
        for (String columnName : columnNames)
            addColumn(new Column(tableName, columnName));
    }
    public void addColumn(Column column) {
        if (!m_columns.contains(column))
            m_columns.add(column);
    }

    public void addColumns(ArrayList<Column> columns) {
        for (Column column : columns) {
            addColumn(column);
        }
    }

    public ArrayList<Column> getColumns() {
        return m_columns;
    }

    public ArrayList<Column> getColumns(Column withoutColumn) {
        ArrayList<Column> ret = new ArrayList<Column>(m_columns);
        ret.remove(withoutColumn);
        return ret;
    }

    public String getTableName() {
        return m_tableName;
    }

    public String [] getColumnNames() {
        String [] res = new String[m_columns.size()];
        for (int i = 0; i < m_columns.size(); i++)
            res[i] = m_columns.get(i).getColumnName();
        return res;
    }

    public String [] getColumnNames(Column withoutColumn) {
        ArrayList<Column> columns = getColumns(withoutColumn);
        String [] res = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++)
            res[i] = columns.get(i).getColumnName();
        return res;
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

    @Override
    public int hashCode() {
        return m_tableName.hashCode() + 32 * m_columns.hashCode();
    }
}
