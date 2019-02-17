package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.meta.Column;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

// Колонка с ключами
// Например account_rk -> [101, 102, 102]
public class ColumnValues {
    private Column m_column;
    private List<String> m_values;

    public ColumnValues(Column column, List<String> values) {
        m_column = column;
        m_values = new ArrayList<String>(new HashSet<String>(values));  // Дедубликация
    }

    public ColumnValues(Column column) {
        m_column = column;
        m_values = new ArrayList<String>();
    }

    public void addVaue(String value) {
        if (!m_values.contains(value))
            m_values.add(value);
    }

    public Column getColumn() {
        return m_column;
    }

    public List<String> getValues() {
        return m_values;
    }
}
