package tinkoff.dwh.cut;

import java.util.List;
import java.util.Map;

public class CutEngine {

    // Метаданные ключей таблиц, где для каждой таблицы перечислин список ключей, обрабатываемых в движке
    private Map<String, List<String>> m_tableKeyMetadata;

    // Метаданные таблиц связей для джобов, где для каждого Job'а перечисленны связи tableFrom,keyFrom<->tableTo,keyTo
    private Map<String, List<TableRelation>> m_jobTableRelations;

    public CutEngine(Map<String, List<String>> tableKeyMetadata, Map<String, List<TableRelation>> jobTableRelations) {
        m_tableKeyMetadata = tableKeyMetadata;
        m_jobTableRelations = jobTableRelations;
    }

    public List<TableRelation> getJobRelations(String jobName) {
        return m_jobTableRelations.get(jobName);
    }
}
