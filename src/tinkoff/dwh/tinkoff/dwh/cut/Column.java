package tinkoff.dwh.cut;

public class Column {
    public String m_tableName;
    public String m_columnName;
    public Column(String tableName, String columnName) {
        m_tableName = tableName;
        m_columnName = columnName;
    }

    @Override
    public boolean equals(Object o) {
        Column c = (Column)o;
        return c.m_tableName.equals(m_tableName) && c.m_columnName.equals(m_columnName);
    }
}

