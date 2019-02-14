package tinkoff.dwh.cut;

import java.util.HashSet;

public class JobTableRelations {
    HashSet<TableRelation> m_relations = new HashSet<TableRelation>();
    HashSet<SingleKey> m_singleKeys = new HashSet<SingleKey>();

    public JobTableRelations() {}

    // Массив вида table_from, column_from, table_to, column_to
    public JobTableRelations(String [][] relations) {
        for (int i = 0; i < relations.length; i++)
            addRelation(new TableRelation(new Column(relations[i][0], relations[i][1]), new Column(relations[i][2], relations[i][3])));
    }

    public void addRelation(TableRelation relation) {
        m_relations.add(relation);

        for (SingleKey singleKey : m_singleKeys) {
            if (singleKey.getKeys().contains(relation.m_left))
        }
    }
}
