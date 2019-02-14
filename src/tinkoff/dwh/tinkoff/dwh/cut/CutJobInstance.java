package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;

import java.util.*;

/* Экзмемпляр запуска расчета согласованного списка ключей */
public class CutJobInstance {

    private AerospikeClient m_client;
    private String m_aerospikeNamespace;
    private String m_jobName;
    private CutEngine m_cutEngine;
    private HashMap<Column, Set<String>> m_keys = new HashMap<Column, Set<String>>();              // Ключи которые копим
    private Set<String> m_processedTables = new HashSet<String>();                                      // Список входных таблиц которые отдали свои данные и эти данные уже обработали здесь
    private ArrayList<CutLinkTable> m_cutLinkTables = new ArrayList<CutLinkTable>();                        // Таблицы связки ключей друг с другом

    public CutJobInstance(AerospikeClient client, String aerospikeNamespace, String jobName, CutEngine cutEngine) {
        m_jobName = jobName;
        m_cutEngine = cutEngine;
        m_client = client;
        m_aerospikeNamespace = aerospikeNamespace;

        // Из метаданных понимаем какие ключи нужно копить и для них создаем пустые массивы, куда будем записывать новые ключи
        for (TableRelation tr : m_cutEngine.getJobRelations(jobName)) {
            Column [] columns = {tr.m_left, tr.m_right};
            for(Column column : columns) {
                if (!m_keys.containsKey(column))
                    m_keys.put(column, new HashSet<String>());
            }
        }

        initCutTables();
    }

    private void initCutTables() {
        HashMap<String, ArrayList<String>> tableXColumns = new HashMap<String, ArrayList<String>>();    // Какие вообще есть таблицы и с какими полями
        for (TableRelation r : m_cutEngine.getJobRelations(m_jobName)) {
            if (!tableXColumns.containsKey(r.m_tableFrom)) tableXColumns.put(r.m_tableFrom, new ArrayList<String>());
            if (!tableXColumns.get(r.m_tableFrom).contains(r.m_keyFrom)) tableXColumns.get(r.m_tableFrom).add(r.m_keyFrom);
            if (!tableXColumns.containsKey(r.m_tableTo)) tableXColumns.put(r.m_tableTo, new ArrayList<String>());
            if (!tableXColumns.get(r.m_tableTo).contains(r.m_keyTo)) tableXColumns.get(r.m_tableTo).add(r.m_keyTo);
        }

        // Пробегаемся по всем таблицам
        // И только для таблиц с более одной колонкой создаем привязываем таблицу связку в Aerospike
        for (String tableName : tableXColumns.keySet()) {
            if (tableXColumns.get(tableName).size() > 1)
                m_cutLinkTables.add(new CutLinkTable(tableName, m_client, m_aerospikeNamespace, tableXColumns.get(tableName)));
        }
    }

    private void addNewLinks(ArrayList<String> columnNames, ArrayList<ArrayList<String>> rows) {
        // TODO: добавить добавление новых связей напрямую в Aerospike
    }

    // Добавить очередной инкремент
    public void putNextTableInc(String tableName, ArrayList<String> columnNames, ArrayList<ArrayList<String>> rows) {

        addNewLinks(columnNames, rows);

        // Транспонируем ключи (+убираем из них дубли)
        HashMap<String, ArrayList<String>> keys = new HashMap<String, ArrayList<String>>();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            keys.put(columnName, new ArrayList<String>());
            for (ArrayList<String> row : rows) {
                String key = row.get(i);
                if (!m_keys.get(columnName).contains(key))
                    keys.get(columnName).add(key);
            }
        }

        for (String columnName : keys.keySet()) {
            addNewKeys(columnName, keys.get(columnName));
        }

        m_processedTables.add(tableName);
    }

    // Вернуть из списка ключей те, которых еще нет в массиве накапливаемых ключей
    private ArrayList<String> findRealNewKeys(String columnName, ArrayList<String> keys) {
        Set<String> existKeys = m_keys.get(columnName);
        ArrayList<String> ret = new ArrayList<String>();
        for (String key : keys) {
            if (!existKeys.contains(key))
                ret.add(key);
        }
        return ret;
    }

    // Добавить новые ключи реукрсивно с учетом поиска в таблицах связей
    public void addNewKeys(String columnName, ArrayList<String> keys) {

        if (keys.size() == 0) return;

        ArrayList<String> newKeys = findRealNewKeys(columnName, keys);
        m_keys.get(columnName).addAll(newKeys);

        for (TableRelation tr : m_cutEngine.getJobRelations(m_jobName)) {
            if (tr.)
        }

        // Для каждой таблицы связок ищем, есть ли в ней наша колонка
        // Для такой таблицы, запускаем рекурсивный поиск вторичных ключей
        for (CutLinkTable linkTable : m_cutLinkTables) {
            ArrayList<String> linkColumns = new ArrayList<String>(linkTable.getColumnNames());
            if (linkColumns.contains(columnName)) {
                linkColumns.remove(columnName);
                for (String secColumnName : linkColumns) {
                    ArrayList<String> secKeys = linkTable.getSecondaryKeys(columnName, keys, secColumnName);
                    addNewKeys(secColumnName, secKeys);
                }
            }
        }
    }

    public ArrayList<CutLinkTable> getCutLinkTables() {
        return m_cutLinkTables;
    }

    public HashMap<String, Set<String>> getKeys() {
        return m_keys;
    }
}
