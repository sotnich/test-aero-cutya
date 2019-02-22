package tinkoff.dwh.cut.meta;

import java.util.ArrayList;

public class TableRelation {

    private Column m_left;
    private Column m_right;
    private String m_type;      // Тип связи - left, inner, full

    public TableRelation(Column left, Column right, String type){
        m_left = left;
        m_right = right;
        m_type = type;
    }

    public Column getLeft() {
        return m_left;
    }

    public Column getRight() {
        return m_right;
    }

    public ArrayList<Column> getColumns() {
        ArrayList<Column> ret = new ArrayList<Column>();
        ret.add(getLeft());
        ret.add(getRight());
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        TableRelation or = (TableRelation) o;
        return or.getLeft().equals(getLeft()) && or.getRight().equals(getRight());
    }

    @Override
    public String toString() {
        return m_left + " -> " + m_right;
    }
}