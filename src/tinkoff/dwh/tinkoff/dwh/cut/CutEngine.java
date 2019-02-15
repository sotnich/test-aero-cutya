package tinkoff.dwh.cut;

import tinkoff.dwh.cut.meta.JobTableRelations;

public class CutEngine {

    // Метаданные таблиц связей для джобов, где для каждого Job'а перечисленны связи tableFrom,keyFrom<->tableTo,keyTo
    private JobTableRelations m_jobTableRelations;

    public CutEngine(JobTableRelations jobTableRelations) {
        m_jobTableRelations = jobTableRelations;
    }

    public JobTableRelations getJobRelations(String jobName) {
        return m_jobTableRelations;
    }
}
