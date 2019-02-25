package tinkoff.dwh.cut.meta;

public class Column {

    private String m_tableName;
    private String m_columnName;
    private boolean m_activeFlg;        // Ключи из этого поля должны попадать в коллекцию без необходимости проверять пересечения (Активное поле)

    public Column(String tableName, String columnName) {
        m_tableName = tableName;
        m_columnName = columnName;
        m_activeFlg = false;
    }

    public Column(String tableName, String columnName, boolean isActive) {
        m_tableName = tableName;
        m_columnName = columnName;
        m_activeFlg = isActive;
    }

    public String getTableName() {
        return m_tableName;
    }

    public String getColumnName() {
        return m_columnName;
    }

    public void setActive() {
        m_activeFlg = true;
    }

    public boolean isActiveFlg() {
        return m_activeFlg;
    }

    @Override
    public boolean equals(Object o) {
        Column c = (Column)o;
        return c.m_tableName.equals(m_tableName) && c.m_columnName.equals(m_columnName);
    }

    @Override
    public int hashCode() {
        return (m_tableName + m_columnName).hashCode();
    }

    @Override
    public String toString() {
        return m_tableName + "." + m_columnName;
    }
}

