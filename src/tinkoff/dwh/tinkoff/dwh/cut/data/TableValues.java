package tinkoff.dwh.cut.data;

import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TableValues {
    private Table m_table;
    private HashMap<Column, ArrayList<String>> m_values;

    public TableValues(Table table) {
        m_table = table;
        m_values = new HashMap<Column, ArrayList<String>>();
        for (Column column : m_table.getColumns())
            m_values.put(column, new ArrayList<String>());
    }

    public void addRow(ArrayList<String> rowValues) {
        for (int i = 0; i < m_table.getColumns().size(); i++)
            m_values.get(m_table.getColumns().get(i)).add(rowValues.get(i));
    }

    public ArrayList<KeyValue> getRow(int i) {
        ArrayList<KeyValue> res = new ArrayList<KeyValue>();
        for (Column column : m_table.getColumns()) {
            res.add(new KeyValue(column, m_values.get(column).get(i)));
        }
        return res;
    }

    public int getRowsCnt() {
        if (m_table.getColumns().size() > 0) {
            return m_values.get(m_table.getColumns().get(0)).size();
        }
        return 0;
    }

    public void addRow(String [] rowValues) {
        for (int i = 0; i < m_table.getColumns().size(); i++)
            m_values.get(m_table.getColumns().get(i)).add(rowValues[i]);
    }

    public ColumnValues getColumnValues(Column column) {
        return new ColumnValues(column, m_values.get(column));
    }

    public List<ColumnValues> getColumnsValues() {
        List ret = new ArrayList<ColumnValues>();
        for (Column column : m_values.keySet())
            ret.add(getColumnValues(column));
        return ret;
    }

    public Table getTable(){
        return m_table;
    }
}
