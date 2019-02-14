package tinkoff.dwh.cut;

public class TableRelation {

    public Column m_left;
    public Column m_right;
    public TableRelation(Column left, Column right){
        m_left = left;
        m_right = right;
    }

}