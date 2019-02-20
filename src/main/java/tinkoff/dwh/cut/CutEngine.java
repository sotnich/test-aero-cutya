package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.JobTableRelations;
import tinkoff.dwh.cut.meta.Table;
import tinkoff.dwh.cut.meta.TableRelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class CutEngine {

    // Метаданные таблиц связей для джобов, где для каждого Job'а перечисленны связи tableFrom,keyFrom<->tableTo,keyTo
    private HashMap<String, JobTableRelations> m_jobTableRelations = new HashMap<String, JobTableRelations>();

    private AerospikeClient m_client;

    private HashMap<String, CutJob> m_cuts = new HashMap<String, CutJob>();

    public CutEngine(AerospikeClient client, HashMap<String, ArrayList<TableRelation>> relations) {
        m_client = client;
        for (String jobName : relations.keySet()) {
            m_jobTableRelations.put(jobName, new JobTableRelations(relations.get(jobName)));
        }
    }

    public JobTableRelations getJobRelations(String jobName) {
        return m_jobTableRelations.get(jobName);
    }

    public ArrayList<String> getJobs() {
        return new ArrayList<String>(m_jobTableRelations.keySet());
    }

    // Вернет таблицы с объединенными полями по всем job'ам
    public ArrayList<Table> getTablesWithMoreThanOneColumn() {
        ArrayList<Table> ret = new ArrayList<Table>();
        for (String jobName : m_jobTableRelations.keySet()) {
            for (Table newTable : m_jobTableRelations.get(jobName).getTablesWithMoreThanOneColumn()) {
                boolean found = false;
                for (Table table : ret) {
                    if (table.getTableName().equals(newTable.getTableName())) {
                        table.addColumns(newTable.getColumns());
                        found = true;
                    }
                }
                if (!found) {
                    ret.add(new Table(newTable));
                }
            }
        }
        return ret;
    }

    public ArrayList<Table> getTables() {
        HashSet<Table> ret = new HashSet<Table>();
        for (String jobName : m_jobTableRelations.keySet()) {
            ret.addAll(m_jobTableRelations.get(jobName).getTables());
        }
        return new ArrayList<Table>(ret);
    }

    public void putInc(TableValues values) {

    }
}
