package tinkoff.dwh.cut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CutEngine {

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


    // Метаданные ключей таблиц, где для каждой таблицы перечислин список ключей, обрабатываемых в движке
    private Map<String, List<String>> m_tableKeyMetadata = new HashMap<String, List<String>>();

    // Метаданные таблиц связей для джобов, где для каждого Job'а перечисленны связи tableFrom,keyFrom<->tableTo,keyTo
    private Map<String, List<TableRelation>> m_jobTableRelations = new HashMap<String, List<TableRelation>>();

    public CutEngine() {
        String tableName = "prod_dds.installment";
        List<String> keys = new ArrayList<String>();
        keys.add("account_rk");
        keys.add("installment_rk");
        m_tableKeyMetadata.put(tableName, keys);

        tableName = "prod_dds.financial_account_chng";
        keys = new ArrayList<String>();
        keys.add("account_rk");
        m_tableKeyMetadata.put(tableName, keys);

        tableName = "prod_dds.financial_account_chng_bal";
        keys = new ArrayList<String>();
        keys.add("account_rk");
        m_tableKeyMetadata.put(tableName, keys);

        String jobName = "EMART 1 LOAD ACCOUNT INSTALLMENT A";
        List<TableRelation> relations = new ArrayList<TableRelation>();
        relations.add(new TableRelation("prod_dds.installment", "installment_rk", "prod_dds.installment", "installment_rk"));
        relations.add(new TableRelation("prod_dds.installment", "account_rk", "prod_dds.financial_account_chng", "account_rk"));
        relations.add(new TableRelation("prod_dds.installment", "account_rk", "prod_dds.financial_account_chng_bal", "account_rk"));
        m_jobTableRelations.put(jobName, relations);
    }

    public List<TableRelation> getJobRelations(String jobName) {
        return m_jobTableRelations.get(jobName);
    }
}
