import com.aerospike.client.*;
import org.junit.Assert;
import tinkoff.dwh.cut.data.KeyValue;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseTest {
//  String m_aerospikeHost = "178.128.134.224";
    String m_aerospikeHost = "ds-aerospike01t.tcsbank.ru";
    String m_testNamespace = "ssd";
    AerospikeClient m_client;

    public BaseTest(String aerospikeHost, String namespace) {
        m_aerospikeHost = aerospikeHost;
        m_testNamespace = namespace;
        m_client = new AerospikeClient(m_aerospikeHost, 3000);
    }

    public BaseTest() {
        m_client = new AerospikeClient(m_aerospikeHost, 3000);
    }

    public void assertArrays(List<String> a1, List<String> a2) {
        Assert.assertTrue(a1.size() == a2.size() && a1.containsAll(a2) && a2.containsAll(a1));
    }

    public void assertArrays(List<String> a1, String ... values) {
        assertArrays(a1, listFromArray(values));
    }

    public ArrayList<KeyValue> getKeyValues(String tableName, String... args) {
        ArrayList<KeyValue> ret = new ArrayList<KeyValue>();
        for (int i = 0; i < args.length; i += 2)
            ret.add(new KeyValue(new Column(tableName, args[i]), args[i+1]));
        return ret;
    }

    public ArrayList<String> getAllKeyValueValues(ColumnsValues row) {
        ArrayList<String> ret = new ArrayList<String>();
        for (Column column : row.getColumns())
            ret.addAll(row.getValues(column));
        return ret;
    }

    public void deleteTable(String tableName) {
        m_client.scanAll(null, m_testNamespace, tableName, new ScanCallback() {
            public void scanCallback(Key key, Record record) throws AerospikeException {
                m_client.delete(null, key);
            }
        });
    }

    public ArrayList<String> listFromArray(String ... values) {
        return new ArrayList<String>(Arrays.asList(values));
    }
}
