package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import tinkoff.dwh.cut.meta.*;

import java.util.*;

/* Экзмемпляр запуска расчета согласованного списка ключей */
public class CutJobInstance {

    private AerospikeClient m_client;
    private String m_aerospikeNamespace;
    private String m_jobName;
    private CutEngine m_cutEngine;
    private HashMap<SingleKey, Set<String>> m_keys = new HashMap<SingleKey, Set<String>>();             // Ключи которые копим
    private Set<String> m_processedTables = new HashSet<String>();                                      // Список входных таблиц которые отдали свои данные и эти данные уже обработали здесь
    private ArrayList<CutLinkTable> m_cutLinkTables = new ArrayList<CutLinkTable>();                    // Таблицы связки ключей друг с другом

    public CutJobInstance(AerospikeClient client, String aerospikeNamespace, String jobName, CutEngine cutEngine) {
        m_jobName = jobName;
        m_cutEngine = cutEngine;
        m_client = client;
        m_aerospikeNamespace = aerospikeNamespace;

        // Из метаданных понимаем какие ключи нужно копить и для них создаем пустые массивы, куда будем записывать новые ключи
        for (SingleKey singleKey : m_cutEngine.getJobRelations(m_jobName).getSingleKeys()) {
            m_keys.put(singleKey, new HashSet<String>());
        }

        // Для таблиц с более одной колонкой создаем таблицу-связку в Aerospike
        for (Table table : m_cutEngine.getJobRelations(m_jobName).getTables()) {
            if (table.getColumns().size() > 1) {
                m_cutLinkTables.add(new CutLinkTable(m_client, m_aerospikeNamespace, table));
            }
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
                if (!keys.get(columnName).contains(key))
                    keys.get(columnName).add(key);
            }
        }

        for (String columnName : keys.keySet()) {
            addNewKeys(new Column(tableName, columnName), keys.get(columnName));
        }

        m_processedTables.add(tableName);
    }


    // Вернуть из списка ключей те, которых еще нет в массиве накапливаемых ключей
    private ArrayList<String> findRealNewKeys(Column column, ArrayList<String> keys) {
        ArrayList<String> ret = new ArrayList<String>();
        for (String key : keys) {
            if (!getKeys(column).contains(key))
                ret.add(key);
        }
        return ret;
    }

    // Вернуть все таблицы-связки в которых есть заданная колонка
    private ArrayList<CutLinkTable> getLinkTables(Column column) {
        ArrayList<CutLinkTable> res = new ArrayList<CutLinkTable>();
        for (CutLinkTable linkTable : m_cutLinkTables) {
            if (linkTable.getTable().getColumns().contains(column))
                res.add(linkTable);
        }
        return res;
    }

    // Добавить новые ключи реукрсивно с учетом поиска в таблицах связей
    public void addNewKeys(Column column, ArrayList<String> keys) {

        if (keys.size() == 0) return;           // Нечего добавлять
        if (getKeys(column) == null) return;    // Ключи по такому полю не копим - по сути ошибка

        ArrayList<String> newKeys = findRealNewKeys(column, keys);
        getKeys(column).addAll(newKeys);

//        for (TableRelation tr : m_cutEngine.getJobRelations(m_jobName)) {
//            if (tr.)
//        }

        // Для каждой таблицы с нашей колонкой запускаем рекурсивный поиск вторичных ключей
//        for (CutLinkTable linkTable : getLinkTables(column)) {
//            ArrayList<String> linkColumns = new ArrayList<String>(linkTable.getColumnNames());
//            if (linkColumns.contains(columnName)) {
//                linkColumns.remove(columnName);
//                for (String secColumnName : linkColumns) {
//                    ArrayList<String> secKeys = linkTable.getSecondaryKeys(columnName, keys, secColumnName);
//                    addNewKeys(secColumnName, secKeys);
//                }
//            }
//        }
    }

    public ArrayList<CutLinkTable> getCutLinkTables() {
        return m_cutLinkTables;
    }

    public HashMap<SingleKey, Set<String>> getKeys() {
        return m_keys;
    }

    public Set<String> getKeys(Column column) {
        for (SingleKey singleKey : m_keys.keySet()) {
            if (singleKey.contains(column))
                return m_keys.get(singleKey);
        }
        return null;
    }
}
