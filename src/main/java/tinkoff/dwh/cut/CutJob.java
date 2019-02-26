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

    public CutJob(AerospikeClient client, String namespace, String jobName, CutEngine cutEngine) {
        m_jobName = jobName;
        m_cutEngine = cutEngine;
        m_client = client;
        m_aerospikeNamespace = namespace;

        m_keys = new SingleKeysValues(m_cutEngine.getJobRelations(m_jobName).getSingleKeys());

        // Для таблиц с более одной колонкой создаем таблицу-связку в Aerospike
        for (Table table : m_cutEngine.getJobRelations(m_jobName).getTablesWithMoreThanOneColumn()) {
            CutLinkTable linkTable = new CutLinkTable(m_client, m_aerospikeNamespace, table);
            m_cutLinkTables.add(linkTable);
            m_keys.addLinkTable(linkTable);

            // key1 left join key2 left join key3
        }
    }

    private void addNewLinks(TableValues values) {
        for (CutLinkTable table : m_cutLinkTables)
            if (table.getTable().getTableName().equals(values.getTable().getTableName())) {
                System.out.println("[cut " + m_jobName + "].addNewLinks " + values.getTable() + " -> " + values.getRowsCnt() + " new rows");
                table.addTableValues(values);
            }
    }

    // Добавить очередной инкремент
    // TODO: Может приходить больше колонок чем нужно - нужно убедиться что они не обрабатываются напрасно
    public void putTable(TableValues values) {

        System.out.println("[cut " + m_jobName + "].putTable " + values.getTable() + " -> " + values.getRowsCnt() + " new rows");

        // TODO: Возможно, не эффективно, возможно, лучше сохранить эти записи в кэше, а записать в таблицы связок потом
        addNewLinks(values);

        for (ColumnValues columnValues: values.getColumnsValues()) {

            // TODO: по хорошему все новые ключи из таблицы связей нужно записать в копилку без условий
            addNewKeys(columnValues, 0);
        }

        m_processedTables.add(values.getTable().getTableName());
    }

    // Добавить новые ключи реукрсивно с учетом поиска в таблицах связей
    public void addNewKeys(ColumnValues values, int recurseNum) {


        // TODO: Надо научиться обрабатывать рекурсивные left связи key1 jeft join key2 left join key3
        // Сейчас ключи key2 могут попасть в копилку безусловно из-за связи key2 left join key3
        // А по хорошему ключи key2 должны проверится на связи key1 left join key2
        // Скорее всего логику нужно выносить в класс SingleKey и туда же выносить linkTables

        Column prmColumn = values.getColumn();

        // оставляем только те, ключи, которых нет в копилке
        values.getValues().removeAll(m_keys.getValues(prmColumn));
        if (values.getValues().size() == 0) return;

        String blanks = new String(new char[recurseNum+1]).replace("\0", ">");
        System.out.println(blanks + "[cut " + m_jobName + "].addNewKey " + prmColumn + " -> " + values.getValues().size() + " new keys");

//        boolean anykey_flg = false;         // Признак того, что нужно тянуть любые ключ
//        if (values.getColumn() == new Column("prod_dds.installment", "installment_rk"))
//            anykey_flg = true;
//
//
        if (prmColumn.isActiveFlg()) { // В копилку нужно положить все вошедшие ключи в независимости ни от чего
            m_keys.getValues(prmColumn).addAll(values.getValues());    // добавляем эти ключи
            System.out.println(blanks + "[cut " + m_jobName + "].addNewKey " + prmColumn + " -> " + values.getValues().size() + " new keys [ADDED]");
        }

        ColumnsValues newLookupColumnsValues = new ColumnsValues();
        for (CutLinkTable linkTable : m_cutLinkTables) {
            for (Column column : m_keys.getSingleKey(prmColumn).getColumns()) {
                if (linkTable.getTable().getColumns().contains(column)) {

                    ColumnsValues linkedValues = linkTable.lookup(column, values.getValues());

                    // TODO: Ключи которые пришли по рекурсии из поиска определенной таблицы не должны опять улетать в эту таблицу в поиск

                    if (!prmColumn.isActiveFlg()) { // в копилку нужно положить только ключи, которые пересекаются по таблице связок
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
