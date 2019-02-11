package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;

import java.util.*;

/* Экзмемпляр запуска расчета согласованного списка ключей */
public class CutJobInstance {

    private AerospikeClient m_client;
    private String m_aerospikeNamespace;
    private String m_jobName;
    private CutEngine m_cutEngine;
    private Map<String, Set<String>> m_keys = new HashMap<String, Set<String>>();       // Ключи которые копим
    private Set<String> m_processedJobs = new HashSet<String>();                        // Список Job'ов которые отдали свои данные и эти данные уже обработали здесь
    private ArrayList<CutAeroTable> m_cutLinkTables = new ArrayList<CutAeroTable>();    // Таблицы связки ключей друг с другом

    public CutJobInstance(AerospikeClient client, String aerospikeNamespace, String jobName, CutEngine cutEngine) {
        m_jobName = jobName;
        m_cutEngine = cutEngine;
        m_client = client;
        m_aerospikeNamespace = aerospikeNamespace;

        // Из метаданных понимаем какие ключи нужно копить и для них создаем пустые массивы, куда будем записывать новые ключи
        for (CutEngine.TableRelation tr : m_cutEngine.getJobRelations(jobName)) {
            if (!m_keys.containsKey(tr.m_keyFrom))
                m_keys.put(tr.m_keyFrom, new HashSet<String>());
            if (!m_keys.containsKey(tr.m_keyTo))
                m_keys.put(tr.m_keyTo, new HashSet<String>());
        }

        initCutTables();
    }

    private void initCutTables() {
        HashMap<String, ArrayList<String>> tableXColumns = new HashMap<String, ArrayList<String>>();    // Какие вообще есть таблицы и с какими полями
        for (CutEngine.TableRelation r : m_cutEngine.getJobRelations(m_jobName)) {
            if (!tableXColumns.containsKey(r.m_tableFrom)) tableXColumns.put(r.m_tableFrom, new ArrayList<String>());
            tableXColumns.get(r.m_tableFrom).add(r.m_keyFrom);
            if (!tableXColumns.containsKey(r.m_tableTo)) tableXColumns.put(r.m_tableTo, new ArrayList<String>());
            tableXColumns.get(r.m_tableTo).add(r.m_keyTo);
        }

        // Пробегаемся по всем таблицам
        // И только для таблиц с более одной колонкой создаем привязываем таблицу связку в Aerospike
        for (String tableName : tableXColumns.keySet()) {
            if (tableXColumns.get(tableName).size() > 1)
                m_cutLinkTables.add(new CutAeroTable(tableName, m_client, m_aerospikeNamespace, tableXColumns.get(tableName)));
        }
    }

    // Добавить очередной инкремент
    public void putNextTableInc(String parJobName, String tableName, ArrayList<String> columnNames, ArrayList<ArrayList<String>> rows) {

        // Добавляем новые связи
        // А заодно формируем списки новых ключей (транспонировкой rows)
        ArrayList<ArrayList<String>> columns = new ArrayList<ArrayList<String>>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.add(new ArrayList<String>());
        }
        for (ArrayList<String> row : rows) {

            // TODO: добавить добавление новых связей напрямую в Aerospike

            // Транспонируем rows в columns
            for (int i = 0; i < columnNames.size(); i++) {
                columns.get(i).add(row.get(i));
            }
        }

        // Добавляем новые ключи из инкремента (рекурсивно)
        for (int i = 0; i < columnNames.size(); i++) {
            addNewKeys(tableName, columnNames.get(i), columns.get(i));
        }

        m_processedJobs.add(parJobName);
    }

    // Добавить новые ключи
    public void addNewKeys(String tableName, String columnName, ArrayList<String> keys) {

        ArrayList<String> newKeys = new ArrayList<String>();    // Действительно новые ключи (которых раньше не было)

        // Добавляем ключи и формируем список действительно новых ключей
        for (String key : keys) {
            if (!m_keys.get(columnName).contains(key)) {
                newKeys.add(key);
                m_keys.get(columnName).add(key);
            }
        }

        // Для каждой таблицы связок ищем, есть ли в ней наша колонка
        // Для такой таблицы, запускаем рекурсивный поиск вторичных ключей
        for (CutAeroTable linkTable : m_cutLinkTables) {
            if (linkTable.getColumnNames().contains(columnName)) {
                for (String secColumnName : linkTable.getColumnNames()) {
                    if (!columnName.equals(secColumnName)) {
                        ArrayList<String> secKeys = linkTable.getSecondaryKeys(columnName, keys, secColumnName);
                    }
                }
            }
        }
    }
}
