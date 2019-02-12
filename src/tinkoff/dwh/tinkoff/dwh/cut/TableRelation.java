package tinkoff.dwh.cut;

public class TableRelation {
    public String m_tableFrom;
    public String m_keyFrom;
    public String m_tableTo;
    public String m_keyTo;
    public TableRelation(String table_from, String key_from, String table_to, String key_to){
        m_tableFrom = table_from;
        m_keyFrom = key_from;
        m_tableTo = table_to;
        m_keyTo = key_to;
    }
}