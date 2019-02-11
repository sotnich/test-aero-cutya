package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.util.ArrayList;
import java.util.HashSet;

public class CutLinkTable {

    private String m_tableName;
    private AerospikeClient m_client;
    private String m_namespace;

    private ArrayList<String> m_columnNames = new ArrayList<String>();        // Названия колонок с ключами

    public CutLinkTable(String tableName, AerospikeClient client, String namespace, ArrayList<String> columnNames) {
        m_tableName = tableName;
        m_namespace = namespace;
        m_client = client;
        m_columnNames = columnNames;
    }

    public ArrayList<String> lookup(String prmName, ArrayList<String> prmKeys, String secName) {
        Key[] keys = new Key[prmKeys.size()];
        for(int i = 0; i < prmKeys.size(); i++)
            keys[i] = new Key(m_namespace, m_tableName, getKeyVal(prmName, prmKeys.get(i)));
        Record[] records = m_client.get(null, keys, secName);

        ArrayList<String> ret = new ArrayList<String>();
        for (Record record : records) {
            if (record != null) {
                ArrayList<String> newKeys = (ArrayList<String>) record.getValue(secName);
                for (String key : newKeys) {
                    if (!ret.contains(key))
                        ret.add(key);
                }
            }
        }
        return ret;
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
    public ArrayList<String> getSecondaryKeys(String prmName, ArrayList<String> prmValues, String secName) {

        // Дедубликация
        ArrayList<String> prmValuesD = new ArrayList<String>(new HashSet<String>(prmValues));

        Key [] keys = new Key[prmValuesD.size()];
        for (int i = 0; i < prmValuesD.size(); i++) {
            keys[i] = getKey(prmName, prmValuesD.get(i));
        }

        Record [] records = m_client.get(null, keys, secName);
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
        return new Key(m_namespace, m_tableName, getKeyVal(prmName, prmVal));
    }

    public ArrayList<String> getColumnNames() {
        return m_columnNames;
    }
}
