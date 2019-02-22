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
    // TODO: Может приходить больше колонок чем нужно - нужно убедиться что они не обрабатываются напрасно
    public void putTable(TableValues values) {

//        addNewLinks(columnNames, rows);

        System.out.println("[cut " + m_jobName + "].AddTable " + values.getTable() + " -> " + values.getRowsCnt() + " new rows");
        for (ColumnValues columnValues: values.getColumnsValues()) {
            addNewKeys(columnValues, 0);
        }

        m_processedTables.add(values.getTable().getTableName());
    }

    // Добавить новые ключи реукрсивно с учетом поиска в таблицах связей
    public void addNewKeys(ColumnValues values, int recurseNum) {

        Column prmColumn = values.getColumn();

        // оставляем только те, ключи, которых нет в копилке
        values.getValues().removeAll(m_keys.getValues(prmColumn));
        if (values.getValues().size() == 0) return;

        String blanks = new String(new char[recurseNum+1]).replace("\0", ">");
        System.out.println(blanks + "[cut " + m_jobName + "].addNewKey " + prmColumn + " -> " + values.getValues().size() + " new keys");

        // TODO: Это жесткий HACK - переделать на анализ связи из relation
        // TODO: связи нужно анализизовать для каждой линктэбле отдельно и оставлять ключи если хотя бы с одной правой пересекся ключ
        boolean anykey_flg = false;         // Признак того, что нужно тянуть любые ключ
        if (values.getColumn() == new Column("prod_dds.installment", "installment_rk"))
            anykey_flg = true;
        if (anykey_flg) { // В копилку нужно положить все вошедшие ключи в независимости ни от чего
            m_keys.getValues(prmColumn).addAll(values.getValues());    // добавляем эти ключи
            System.out.println(blanks + "[cut " + m_jobName + "].addNewKey " + prmColumn + " -> " + values.getValues().size() + " new keys [ADDED]");
        }

        ColumnsValues newLookupColumnsValues = new ColumnsValues();
        for (CutLinkTable linkTable : m_cutLinkTables) {
            for (Column column : m_keys.getSingleKey(prmColumn).getColumns()) {
                if (linkTable.getTable().getColumns().contains(column)) {
                    ColumnsValues linkedValues = linkTable.lookup(column, values.getValues());

                    // TODO: Ключи которые пришли по рекурсии из поиска определенной таблицы не должны опять улетать в эту таблицу в поиск

                    if (!anykey_flg) { // в копилку нужно положить только ключи, которые пересекаются по таблице связок
                        m_keys.getValues(prmColumn).addAll(linkedValues.getValues(column));
                        System.out.println(blanks + "[cut " + m_jobName + "].addNewKey " + prmColumn + " -> " + (values.getValues().size() - linkedValues.getValues(column).size()) + " new keys [TRASHED]");
                        System.out.println(blanks + "[cut " + m_jobName + "].addNewKey " + prmColumn + " -> " + linkedValues.getValues(column).size() + " new keys [ADDED]");
                    }
                    linkedValues.removeColumn(column);

                    newLookupColumnsValues.add(linkedValues);
                }
            }
        }

        for (ColumnValues newLookupColumnValues : newLookupColumnsValues.getValues())
            addNewKeys(newLookupColumnValues, recurseNum+1);
    }

    public ArrayList<CutLinkTable> getCutLinkTables() {
        return m_cutLinkTables;
    }

    public SingleKeysValues getKeys() {
        return m_keys;
    }
}
