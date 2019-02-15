package tinkoff.dwh.cut.meta;

import javafx.scene.control.Tab;

import java.util.ArrayList;

public class JobTableRelations {

    ArrayList<TableRelation> m_relations = new ArrayList<TableRelation>();  // Все отношения
    ArrayList<SingleKey> m_singleKeys = new ArrayList<SingleKey>();         // Все SingleKey из отношений
    ArrayList<Column> m_columns = new ArrayList<Column>();                  // Все колонки из отношений
    ArrayList<Table> m_tables = new ArrayList<Table>();                     // Все таблицы из отношений

    public JobTableRelations(ArrayList<TableRelation> relations) {
        for (TableRelation r : relations) {
            addRelation(r);
        }
    }

    // Массив вида table_from, column_from, table_to, column_to
    public JobTableRelations(String [][] relations) {
        for (int i = 0; i < relations.length; i++)
            addRelation(new TableRelation(new Column(relations[i][0], relations[i][1]), new Column(relations[i][2], relations[i][3])));
    }

    private void addColumn(TableRelation relation) {
        for (Column column : relation.getColumns()) {
            if (!m_columns.contains(column))
                m_columns.add(column);
        }
    }

    private void addSingleKey(TableRelation relation) {
        SingleKey nSingleKey = null;
        for (SingleKey singleKey : m_singleKeys) {
            if (singleKey.contains(relation.getLeft()) || singleKey.contains(relation.getRight()))
                nSingleKey = singleKey;
        }
        if (nSingleKey == null) {
            nSingleKey = new SingleKey();
            m_singleKeys.add(nSingleKey);
        }
        nSingleKey.addColumn(relation.getLeft());
        nSingleKey.addColumn(relation.getRight());
    }

    private void addTable(TableRelation relation) {
        for (Column column : relation.getColumns()) {
            Table nTable = null;
            for (Table table : m_tables) {
                if (table.getTableName() == column.getTableName())
                    nTable = table;
            }
            if (nTable == null) {
                nTable = new Table(column.getTableName());
                m_tables.add(nTable);
            }
            nTable.addColumn(column);
        }
    }

    private void addRelation(TableRelation relation) {
        if (!m_relations.contains(relation))
            m_relations.add(relation);
        addSingleKey(relation);
        addColumn(relation);
        addTable(relation);
    }

    public ArrayList<SingleKey> getSingleKeys() {
        return m_singleKeys;
    }

    public ArrayList<TableRelation> getRelations() {
        return m_relations;
    }

    public ArrayList<Table> getTables() {
        return m_tables;
    }
}
