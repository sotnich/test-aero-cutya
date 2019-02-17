package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.meta.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

// Для нескольких колонок привязанные значения
// Например
//  account_rk -> [101,102]
//  statement_kr -> [201,202,203]
public class ColumnsValues {
    private HashMap<Column, ArrayList<String>> m_columnsWighValues = new HashMap<Column, ArrayList<String>>();

    public ColumnsValues() {
    }

    public ColumnsValues(ArrayList<Column> columns, ArrayList<String> values) {
        for (int i = 0; i < columns.size(); i++)
            m_columnsWighValues.put(columns.get(i), new ArrayList<String>(Arrays.asList(values.get(i))));
    }

    public ColumnsValues(ArrayList<Column> columns) {
        for (Column column : columns)
            m_columnsWighValues.put(column, new ArrayList<String>());
    }

    public ArrayList<Column> getColumns() {
        return new ArrayList<Column>(m_columnsWighValues.keySet());
    }

    public ArrayList<String> getValues(Column column) {
        return m_columnsWighValues.get(column);
    }

    public int addColumnValues(Column column, ArrayList<String> newValues) {
        ArrayList<String> values = m_columnsWighValues.get(column);
        int addCnt = 0;
        if (values == null) {
            values = new ArrayList<String>();
            m_columnsWighValues.put(column, values);
        }
        for (String newValue : newValues) {
            if (!values.contains(newValue)) {
                values.add(newValue);
                addCnt++;
            }
        }
        return addCnt;
    }

    public int addColumnValue(Column column, String value) {
        ArrayList<String> values = m_columnsWighValues.get(column);
        if (values == null) {
            values = new ArrayList<String>();
            m_columnsWighValues.put(column, values);
        }
        if (values.contains(value))
            return 0;
        else {
            values.add(value);
            return 1;
        }
    }

    public int deleteColumnValue(Column column, String value) {
        if (m_columnsWighValues.get(column).contains(value))
            return 0;
        else {
            m_columnsWighValues.get(column).remove(value);
            return 1;
        }
    }

    public void add(ColumnsValues columnsValues) {
        for (Column column : columnsValues.getColumns())
            addColumnValues(column, columnsValues.getValues(column));
    }

    public int add(ArrayList<KeyValue> row) {
        int addCnt = 0;
        for (KeyValue kv : row)
            addCnt += addColumnValue(kv.getKey(), kv.getValue());
        return addCnt;
    }

    public int delete(ArrayList<KeyValue> row) {
        int delCnt = 0;
        for (KeyValue kv : row)
            delCnt += deleteColumnValue(kv.getKey(), kv.getValue());
        return delCnt;
    }

    public List<ColumnValues> getValues() {
        List<ColumnValues> ret = new ArrayList<ColumnValues>();
        for (Column column : m_columnsWighValues.keySet())
            ret.add(new ColumnValues(column, getValues(column)));
        return ret;
    }
}
