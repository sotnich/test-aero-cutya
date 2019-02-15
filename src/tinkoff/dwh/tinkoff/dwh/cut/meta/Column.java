package tinkoff.dwh.cut.meta;

import java.util.ArrayList;

public class Column {

    private String m_tableName;
    private String m_columnName;

    public Column(String tableName, String columnName) {
        m_tableName = tableName;
        m_columnName = columnName;
    }

    public String getTableName() {
        return m_tableName;
    }

    public String getColumnName() {
        return m_columnName;
    }

    public static ArrayList<Column> copyColumnsButOne(ArrayList<Column> columns, Column oneColumn) {
        ArrayList<Column> res = new ArrayList<Column>(columns);
        columns.remove(oneColumn);
        return res;
    }

    @Override
    public boolean equals(Object o) {
        Column c = (Column)o;
        return c.m_tableName.equals(m_tableName) && c.m_columnName.equals(m_columnName);
    }

    @Override
    public String toString() {
        return m_tableName + "." + m_columnName;
    }
}

