package tinkoff.dwh.cut;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.meta.Column;

import java.util.ArrayList;
import java.util.List;

public class ColumnsValuesLoaderThread extends Thread {
    private List<String> m_valuesInput;
    private Column m_prmColumn;
    private ColumnsValues m_result;
    private CutLinkTable m_linkTable;

    public ColumnsValuesLoaderThread(Column prmColumn, List<String> values, CutLinkTable linkTable) {
        m_prmColumn = prmColumn;
        m_valuesInput = values;
        m_linkTable = linkTable;
    }

    public void run() {
        m_result = m_linkTable.getReference(m_prmColumn, m_valuesInput);
    }

    public ColumnsValues getValues() {
        return m_result;
    }
}
