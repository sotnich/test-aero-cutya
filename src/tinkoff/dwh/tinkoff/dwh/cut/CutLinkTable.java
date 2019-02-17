package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import tinkoff.dwh.cut.data.KeyValue;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;
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

    public ColumnsValues lookup(KeyValue key) {
        return getReference(key);
    }

    public ColumnsValues lookup(Column prm, List<String> values) {

        // Дедубликация входящих ключей и формирование списка ключей для поиска
        ArrayList<String> prmValuesD = new ArrayList<String>(new HashSet<String>(values));
        Key [] keys = new Key[prmValuesD.size()];
        for (int i = 0; i < prmValuesD.size(); i++) {
            keys[i] = getKey(new KeyValue(prm, prmValuesD.get(i)));
        }

        ColumnsValues ret = new ColumnsValues(m_table.getColumns(prm));
        Record [] records = m_client.get(null, keys, m_table.getColumnNames(prm));
        for (Record record : records)
            ret.add(getRowFromRecord(record, m_table.getColumns(prm)));

        return ret;
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
        if (bins.size() > 0)
            m_client.put(null, getKey(prm), bins.toArray(new Bin[0]));
        else
            m_client.delete(null, getKey(prm));
    }

    private ColumnsValues getReference(KeyValue prm) {
        Record record = m_client.get(null, getKey(prm), m_table.getColumnNames(prm.getKey()));
        return getRowFromRecord(record, m_table.getColumns(prm.getKey()));
    }

    private void deleteReference(KeyValue prm, ArrayList<KeyValue> row) {
        if (prm.isNonEmpty() && row.size() > 0) {
            ColumnsValues kvRow = getReference(prm);
            if (kvRow.delete(row) > 0)
                setReference(prm, kvRow);
        }
    }

    private ColumnsValues getRowFromRecord(Record record, ArrayList<Column> columns) {
        ColumnsValues ret = new ColumnsValues(columns);
        if (record != null) {
            for (Column column : columns) {
                ret.addColumnValues(column, (ArrayList<String>) record.getValue(column.getColumnName()));
            }
        }
        return ret;
    }

//    public void deleteSecondaryKey(Column prmColumn, String prmVal, Column secColumn, String secKey) {
//        ArrayList<String> alreadyKeys = getSecondaryKeys(prmColumn, prmVal, secColumn);
//        if (!alreadyKeys.contains(secKey)) return;
//
//        alreadyKeys.remove(secKey);
//
//        ArrayList<String> newKeys = new ArrayList<String>(alreadyKeys);
//
//        Bin secBin = new Bin(secColumn.getColumnName(), alreadyKeys);
//        m_client.put(null, getKey(prmColumn, prmVal), secBin);
//    }
//
//    public void putSecondaryKey(Column prmColumn, String prmVal, Column secColumn, String secKey) {
//        ArrayList<String> keys = new ArrayList<String>();
//        keys.add(secKey);
//        putSecondaryKeys(prmColumn, prmVal, secName, keys);
//    }

//    public void putSecondaryKeys(Column prmName, String prmVal, HashMap<Column, ArrayList<String>> secVals) {
//        HashMap<Column, ArrayList<String>> alreadyKeys = getSecondaryKeys(prmName, prmVal);
//
//        for (Column column : secVals.keySet()) {
//            for (String key : alreadyKeys.get(column)) {
//                if (!secKeys.contains(key))
//                    secKeys.add(key);
//            }
//        }
//
//        Bin prmBin = new Bin(prmName, prmVal);
//        Bin secBin = new Bin(secName, secKeys);
//        m_client.put(null, getKey(prmName, prmVal), prmBin, secBin);
//    }
//
//    private ArrayList<Column> getSecondaryColumns(Column primaryColumn) {
//        ArrayList<Column> secColumns = new ArrayList<Column>(m_table.getColumns());
//        secColumns.remove(primaryColumn);
//        return secColumns;
//    }
//
//    private String [] getSecondaryColumnNames(Column primaryColumn) {
//        ArrayList<Column> secColumns = getSecondaryColumns(primaryColumn);
//        String [] secColumnNames = new String [secColumns.size()];
//        for (int i = 0; i < secColumns.size(); i++)
//            secColumnNames[i] = secColumns.get(i).getColumnName();
//        return secColumnNames;
//    }
//
//    private ArrayList<String> getSecondaryKeys(Column prmColumn, String prmValue, Column secColumn) {
//        ArrayList<String> ret = new ArrayList<String>();
//        Record record = m_client.get(null, getKey(prmColumn, prmValue), secColumn.getColumnName());
//        if (record == null)
//            return ret;
//        else {
//                ret = (ArrayList<String>) record.getValue(secColumn.getColumnName());
//        }
//        return ret;
//    }
//    public HashMap<Column, ArrayList<String>> getSecondaryKeys(Column prmColumn, String prmValue) {
//        HashMap<Column, ArrayList<String>> ret = new HashMap<Column, ArrayList<String>>();
//        Record record = m_client.get(null, getKey(prmColumn, prmValue), getSecondaryColumnNames(prmColumn));
//        if (record == null)
//            return ret;
//        else {
//            for (Column column : getSecondaryColumns(prmColumn)) {
//                ret.put(column, (ArrayList<String>) record.getValue(column.getColumnName()))
//            }
//        }
//        return ret;
//    }
//
//    public HashMap<Column, ArrayList<String>> getSecondaryKeys(Column prmColumn, ArrayList<String> prmValues) {
//
//        // Дедубликация входящих ключей
//        ArrayList<String> prmValuesD = new ArrayList<String>(new HashSet<String>(prmValues));
//
//        Key [] keys = new Key[prmValuesD.size()];
//        for (int i = 0; i < prmValuesD.size(); i++) {
//            keys[i] = getKey(prmColumn, prmValuesD.get(i));
//        }
//
//        Record [] records = m_client.get(null, keys, getSecondaryColumnNames(prmColumn));
//        HashMap<Column, ArrayList<String>> ret = new HashMap<Column, ArrayList<String>>();
//
//        for (Record record : records) {
//            if (record != null) {
//                for (Column column : getSecondaryColumns(prmColumn)) {
//
//                    ArrayList<String> vals;
//                    vals = ret.get(column);
//                    if (vals == null) {
//                        ret.put(column, (ArrayList<String>) record.getValue(column.getColumnName()));
//                    }
//                    else {
//                        for (String val : (ArrayList<String>) record.getValue(column.getColumnName())) {
//                            if (!vals.contains(val))
//                                vals.add(val);
//                        }
//                    }
//                }
//            }
//        }
//
//        return ret;
//    }

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

    private String getKeyVal(KeyValue kv) {
        return getShortKeyName(kv.getKey().getColumnName())+kv.getValue();
    }

    private Key getKey(KeyValue kv) {
        return new Key(m_namespace, m_table.getTableName(), getKeyVal(kv));
    }

    public Table getTable() {
        return m_table;
    }
}
