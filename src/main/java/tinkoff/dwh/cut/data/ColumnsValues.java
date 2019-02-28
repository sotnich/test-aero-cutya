package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.meta.Column;

import java.util.*;

// Для нескольких колонок привязанные значения
// Например
//  account_rk -> [101,102]
//  statement_kr -> [201,202,203]
public class ColumnsValues {
    private HashMap<Column, HashSet<String>> m_columnsWithValues = new HashMap<Column, HashSet<String>>();

    public ColumnsValues() {
    }

    public ColumnsValues(Column [] columns, HashSet<String> [] values) {
        for (int i = 0; i < columns.length; i++)
            m_columnsWithValues.put(columns[i], values[i]);
    }

    public ColumnsValues(ArrayList<Column> columns) {
        for (Column column : columns)
            m_columnsWithValues.put(column, new HashSet<String>());
    }

    public ArrayList<Column> getColumns() {
        return new ArrayList<Column>(m_columnsWithValues.keySet());
    }

    public void removeColumn(Column column) {
        m_columnsWithValues.remove(column);
    }

    public HashSet<String> getValues(Column column) {
        return m_columnsWithValues.get(column);
    }

    public void addColumnValues(Column column, Set<String> newValues) {
        HashSet<String> values = m_columnsWithValues.get(column);
        if (values == null) {
            values = new HashSet<String>();
            m_columnsWithValues.put(column, values);
        }
        values.addAll(newValues);
    }

    public void addColumnValue(Column column, String value) {
        HashSet<String> values = m_columnsWithValues.get(column);
        if (values == null) {
            values = new HashSet<String>();
            m_columnsWithValues.put(column, values);
        }
        values.add(value);
    }

    public int deleteColumnValue(Column column, String value) {
        if (m_columnsWithValues.get(column).contains(value))
            return 0;
        else {
            m_columnsWithValues.get(column).remove(value);
            return 1;
        }
    }

    public void add(ColumnsValues columnsValues) {
        for (Column column : columnsValues.getColumns())
            addColumnValues(column, columnsValues.getValues(column));
    }

    public void add(ArrayList<KeyValue> row) {
        for (KeyValue kv : row)
            addColumnValue(kv.getKey(), kv.getValue());
    }

    public int delete(ArrayList<KeyValue> row) {
        int delCnt = 0;
        for (KeyValue kv : row)
            delCnt += deleteColumnValue(kv.getKey(), kv.getValue());
        return delCnt;
    }

    public List<ColumnValues> getValues() {
        List<ColumnValues> ret = new ArrayList<ColumnValues>();
        for (Column column : m_columnsWithValues.keySet())
            ret.add(new ColumnValues(column, getValues(column)));
        return ret;
    }

    public int getSize() {
        int ret = 0;
        for (Column column : m_columnsWithValues.keySet())
            ret += m_columnsWithValues.get(column).size();
        return ret;
    }
}
