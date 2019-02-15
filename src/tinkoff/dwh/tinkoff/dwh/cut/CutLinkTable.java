package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import static tinkoff.dwh.cut.meta.Column.copyColumnsButOne;

public class CutLinkTable {

    private AerospikeClient m_client;
    private String m_namespace;
    private Table m_table;

    public CutLinkTable(AerospikeClient client, String namespace, Table table) {
        m_table = table;
        m_namespace = namespace;
        m_client = client;
    }

    public void deleteSecondaryKey(String prmName, String prmVal, String secName, String secKey) {
        ArrayList<String> alreadyKeys = getSecondaryKeys(prmName, prmVal, secName);
        if (!alreadyKeys.contains(secKey)) return;

        alreadyKeys.remove(secKey);

        ArrayList<String> newKeys = new ArrayList<String>(alreadyKeys);

        Bin secBin = new Bin(secName, alreadyKeys);
        m_client.put(null, getKey(prmName, prmVal), secBin);
    }

    public void putSecondaryKey(String prmName, String prmVal, String secName, String secKey) {
        ArrayList<String> keys = new ArrayList<String>();
        keys.add(secKey);
        putSecondaryKeys(prmName, prmVal, secName, keys);
    }

    public void putSecondaryKeys(String prmName, String prmVal, String secName, ArrayList<String> secKeys) {
        ArrayList<String> alreadyKeys = getSecondaryKeys(prmName, prmVal, secName);

        for (String key : alreadyKeys) {
            if (!secKeys.contains(key))
                secKeys.add(key);
        }

        Bin prmBin = new Bin(prmName, prmVal);
        Bin secBin = new Bin(secName, secKeys);
        m_client.put(null, getKey(prmName, prmVal), prmBin, secBin);
    }

    public ArrayList<String> getSecondaryKeys(String prmName, String prmValue, String secName) {
        Record record = m_client.get(null, getKey(prmName, prmValue), secName);
        if (record == null)
            return new ArrayList<String>();
        else
            return (ArrayList<String>) record.getValue(secName);
    }

    // TODO: если несколько вторичных ключей, то придется несколько раз вызывать эту функцию для каждого вторичного ключа
    // TODO: По идее можно сделать чтобы можно было возвращать несколько списков ключей в return и вызывать один раз
    public ArrayList<String> getSecondaryKeys(Column prmColumn, ArrayList<String> prmValues) {

        // Дедубликация
        ArrayList<String> prmValuesD = new ArrayList<String>(new HashSet<String>(prmValues));

        Key [] keys = new Key[prmValuesD.size()];
        for (int i = 0; i < prmValuesD.size(); i++) {
            keys[i] = getKey(prmColumn.getColumnName(), prmValuesD.get(i));
        }

        ArrayList<Column> secColumns = Column.copyColumnsButOne(m_table.getColumns(), prmColumn);
        String [] secColumnNames = new String [secColumns.size()];
        for (int i = 0; i < secColumns.size(); i++)
            secColumnNames[i] = secColumns.get(i).getColumnName();

        Record [] records = m_client.get(null, keys, secColumnNames);
        ArrayList<String> ret = new ArrayList<String>();

        for (Record record : records) {
            if (record != null)
                for (String secKey : (ArrayList<String>)record.getValue(secName)) {
                    if (!ret.contains(secKey)) {
                        ret.add(secKey);
                    }
                }
        }

        return ret;
    }

    private String getShortKeyName(String keyName) {
        if (keyName.equals("account_rk"))
            return "ACCN";
        else if (keyName.equals("installment_rk"))
            return "INST";
        else return keyName;
    }

    private String getKeyVal(String prmName, String prmVal) {
        return getShortKeyName(prmName)+prmVal;
    }

    private Key getKey(String prmName, String prmVal) {
        return new Key(m_namespace, m_table.getTableName(), getKeyVal(prmName, prmVal));
    }

    public Table getTable() {
        return m_table;
    }
}
