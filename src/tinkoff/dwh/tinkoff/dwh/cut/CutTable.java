package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.util.ArrayList;

public class CutTable {

    private String m_tableName;
    private AerospikeClient m_client;
    private String m_namespace;

    public CutTable(String tableName, AerospikeClient client, String namespace) {
        m_tableName = tableName;
        m_namespace = namespace;
        m_client = client;
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
}
