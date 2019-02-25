package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.data.KeyValue;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class CutLinkTable {

    private AerospikeClient m_client;
    private String m_namespace;
    private Table m_table;

    public CutLinkTable(AerospikeClient client, String namespace, Table table) {
        m_table = table;
        m_namespace = namespace;
        m_client = client;
    }

    public void addTableValues(TableValues values) {
        if (!values.getTable().equals(m_table)) return;

        for (int i = 0; i < values.getRowsCnt(); i++)
            addRow(values.getRow(i));
    }

    public void addRow(ArrayList<KeyValue> row) {
        for (KeyValue kv : row) {
            addReference(kv, KeyValue.copyNonEmptyWithout(row, kv));
        }
    }

    public void deleteRow(ArrayList<KeyValue> row) {
        for (KeyValue kv : row) {
            deleteReference(kv, KeyValue.copyNonEmptyWithout(row, kv));
        }
    }

    public ColumnsValues lookup(Column prm, List<String> values) {
        ArrayList<String> prmValuesD = new ArrayList<String>(new HashSet<String>(values));  // Дедубликация входящих ключей

        if (prmValuesD.size() <= 30000)
            return getReference(prm, prmValuesD);
        else {
            int threadCnt = 10;
            int batchSize = prmValuesD.size() / threadCnt;
            ColumnsValuesLoaderThread [] threads = new ColumnsValuesLoaderThread[threadCnt];
            for (int i = 0; i < threadCnt; i++) {
                List<String> subList = prmValuesD.subList(i * batchSize, Math.min(prmValuesD.size(), ((i + 1) * batchSize) + 1) );
                threads[i] = new ColumnsValuesLoaderThread(prm, subList, this);
                threads[i].start();
            }

            ColumnsValues ret = new ColumnsValues();
            try {
                for (int i = 0; i < threadCnt; i++) {
                    threads[i].join();
                    ret.add(threads[i].getValues());
                }
            }
            catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
            return ret;
        }
    }

    private void addReference(KeyValue prm, ArrayList<KeyValue> row) {
        if (prm.isNonEmpty() && row.size() > 0) {
            ColumnsValues kvRow = getReference(prm);
            if (kvRow.add(row) > 0)
                setReference(prm, kvRow);
        }
    }

    private void setReference(KeyValue prm, ColumnsValues row) {
        ArrayList<Bin> bins = new ArrayList<Bin>();
        for (Column column : m_table.getColumns(prm.getKey())) {
            if (row.getValues(column).size() > 0)
                bins.add(new Bin(column.getColumnName(), row.getValues(column)));
        }
        bins.add(new Bin(prm.getKey().getColumnName(), prm.getValue()));
        if (bins.size() > 1)
            m_client.put(null, getKey(prm), bins.toArray(new Bin[0]));
        else
            m_client.delete(null, getKey(prm));
    }

    private ColumnsValues getReference(KeyValue prm) {
        Record record = m_client.get(null, getKey(prm), m_table.getColumnNames(prm.getKey()));
        return getRowFromRecord(record, null, m_table.getColumns(prm.getKey()));
    }

    public ColumnsValues getReference(Column prmColumn, List<String> values) {

        ColumnsValues ret = new ColumnsValues(m_table.getColumns());
        if (values.size() == 0) return ret;

        int batchSize = 5000;
        for (int offset = 0; offset < values.size(); offset += batchSize) {
            int size = Math.min(batchSize, values.size() - offset);
            Key[] keys = new Key[size];
            for (int i = 0; i < size; i++)
                keys[i] = getKey(prmColumn, values.get(offset + i));

            Record[] records = m_client.get(null, keys, m_table.getColumnNames());
            boolean [] exists = m_client.exists(null, keys);
            for (Record record : records) {
                if (record != null)
                    ret.add(getRowFromRecord(record, prmColumn, m_table.getColumns(prmColumn)));
            }
        }
        return ret;
    }

    private void deleteReference(KeyValue prm, ArrayList<KeyValue> row) {
        if (prm.isNonEmpty() && row.size() > 0) {
            ColumnsValues kvRow = getReference(prm);
            if (kvRow.delete(row) > 0)
                setReference(prm, kvRow);
        }
    }

    @SuppressWarnings("unchecked")
    private ColumnsValues getRowFromRecord(Record record, Column prmColumn, ArrayList<Column> secColumns) {
        ColumnsValues ret = new ColumnsValues(secColumns);
        if (record != null) {
            for (Column column : secColumns) {
                ret.addColumnValues(column, (ArrayList<String>) record.getValue(column.getColumnName()));
            }
            if (prmColumn != null)
                ret.addColumnValue(prmColumn, record.getString(prmColumn.getColumnName()));
        }
        return ret;
    }

    private String getShortKeyName(String keyName) {
        if (keyName.equals("account_rk"))
            return "ACCN";
        else if (keyName.equals("installment_rk"))
            return "INST";
        else if (keyName.equals("integration_id"))
            return "INTI";
        else if (keyName.equals("application_id"))
            return "APPI";
        else return keyName;
    }

    private String getKeyVal(Column column, String value) {
        return getShortKeyName(column.getColumnName())+value;
    }

    private Key getKey(KeyValue kv) {
        return new Key(m_namespace, m_table.getTableName(), getKeyVal(kv.getKey(), kv.getValue()));
    }

    private Key getKey(Column column, String value) {
        return new Key(m_namespace, column.getTableName(), getKeyVal(column, value));
    }

    public Table getTable() {
        return m_table;
    }
}
