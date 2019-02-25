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
    private String m_namespace;

    private HashMap<String, CutJob> m_cuts = new HashMap<String, CutJob>();

    public CutEngine(AerospikeClient client, String namespace, HashMap<String, ArrayList<TableRelation>> relations) {
        m_client = client;
        m_namespace = namespace;
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

    public void initTable(String tableName, String [][] values) {
        Table loadTable = new Table(tableName, values[0]);
        CutLinkTable linkTable = new CutLinkTable(m_client, m_namespace, loadTable);
        Utils.initLinkTable(linkTable, values);
    }

    // Вернет таблицы с объединенными полями по всем job'ам
    public ArrayList<Table> getTablesWithMoreThanOneColumn(String jobName) {
        if (jobName == null) return getTablesWithMoreThanOneColumn();
        ArrayList<Table> ret = new ArrayList<Table>();
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
        return ret;
    }

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

    public ArrayList<Table> getTables(String jobName) {
        if (jobName == null) return getTables();
        ArrayList<Table> ret = new ArrayList<Table>();
        for (Table newTable : m_jobTableRelations.get(jobName).getTables()) {
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
        return ret;
    }

    public Table getTable(String tableName) {
        for (Table table : getTables()) {
            if (table.getTableName().equals(tableName))
                return table;
        }
        return null;
    }

    public ArrayList<Table> getTables() {
        ArrayList<Table> ret = new ArrayList<Table>();
        for (String jobName : m_jobTableRelations.keySet()) {
            for (Table newTable : m_jobTableRelations.get(jobName).getTables()) {
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
        return new ArrayList<Table>(ret);
    }

    // Получить список Job'ов которым нужна таблица
    public ArrayList<String> getJobs(Table table) {
        ArrayList<String> ret = new ArrayList<String>();
        for (String jobName : m_jobTableRelations.keySet()) {
            if (m_jobTableRelations.get(jobName).getTables().contains(table))
                ret.add(jobName);
        }
        return ret;
    }

    public ArrayList<CutJob> getCuts() {
        return new ArrayList<CutJob>(m_cuts.values());
    }

    public void putInc(TableValues values) {
        for (String jobName : getJobs(values.getTable())) {
            if (!m_cuts.containsKey(jobName))
                m_cuts.put(jobName, new CutJob(m_client, m_namespace, jobName, this));
            m_cuts.get(jobName).putTable(values);
        }
    }
}
