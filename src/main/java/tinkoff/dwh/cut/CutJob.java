package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import tinkoff.dwh.cut.data.ColumnValues;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.data.SingleKeysValues;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.*;

import java.util.*;

/* Экзмемпляр запуска расчета согласованного списка ключей */
public class CutJob {

    private AerospikeClient m_client;
    private String m_aerospikeNamespace;
    private String m_jobName;

    private CutEngine m_cutEngine;
    private SingleKeysValues m_keys;                                     // Ключи которые копим
    private Set<String> m_processedTables = new HashSet<String>();                                      // Список входных таблиц которые отдали свои данные и эти данные уже обработали здесь
    private ArrayList<CutLinkTable> m_cutLinkTables = new ArrayList<CutLinkTable>();                    // Таблицы связки ключей друг с другом

    public CutJob(AerospikeClient client, String aerospikeNamespace, String jobName, CutEngine cutEngine) {
        m_jobName = jobName;
        m_cutEngine = cutEngine;
        m_client = client;
        m_aerospikeNamespace = aerospikeNamespace;

        m_keys = new SingleKeysValues(m_cutEngine.getJobRelations(m_jobName).getSingleKeys());

        // Для таблиц с более одной колонкой создаем таблицу-связку в Aerospike
        for (Table table : m_cutEngine.getJobRelations(m_jobName).getTablesWithMoreThanOneColumn()) {
            m_cutLinkTables.add(new CutLinkTable(m_client, m_aerospikeNamespace, table));
        }
    }

    private void addNewLinks(TableValues values) {
        if (values.getTable().getColumns().size() <= 1) return;
        // TODO: добавить добавление новых связей напрямую в Aerospike
    }

    // Добавить очередной инкремент
    public void putTable(TableValues values) {

//        addNewLinks(columnNames, rows);

        for (ColumnValues columnValues: values.getColumnsValues()) {
            addNewKeys(columnValues);
        }

        m_processedTables.add(values.getTable().getTableName());
    }

    // Добавить новые ключи реукрсивно с учетом поиска в таблицах связей
    public void addNewKeys(ColumnValues values) {

        ColumnValues newValues = m_keys.addValues(values);
        if (newValues.getValues().size() == 0) return;

        ColumnsValues newLookupColumnsValues = new ColumnsValues();
        for (CutLinkTable linkTable : m_cutLinkTables) {
            for (Column column : m_keys.getSingleKey(newValues.getColumn()).getColumns()) {
                if (linkTable.getTable().getColumns().contains(column)) {
                    newLookupColumnsValues.add(linkTable.lookup(column, newValues.getValues()));
                }
            }
        }

        for (ColumnValues newLookupColumnValues : newLookupColumnsValues.getValues())
            addNewKeys(newLookupColumnValues);
    }

    public ArrayList<CutLinkTable> getCutLinkTables() {
        return m_cutLinkTables;
    }

    public SingleKeysValues getKeys() {
        return m_keys;
    }
}
