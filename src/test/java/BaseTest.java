import com.aerospike.client.AerospikeClient;
import org.junit.Assert;
import tinkoff.dwh.cut.KeyValue;
import tinkoff.dwh.cut.KeyValueRow;
import tinkoff.dwh.cut.meta.Column;

import java.util.ArrayList;
import java.util.List;

public class BaseTest {
    static final String m_aerospikeHost = "178.128.134.224";
//    static final String m_aerospikeHost = "10.216.193.85";
    static final String m_testNamespace = "test";
    AerospikeClient m_client;

    public BaseTest() {
        m_client = new AerospikeClient(m_aerospikeHost, 3000);
    }

    public void assertArrays(List<String> a1, List<String> a2) {
        Assert.assertTrue(a1.size() == a2.size() && a1.containsAll(a2) && a2.containsAll(a1));
    }

    public ArrayList<KeyValue> getKeyValues(String tableName, String... args) {
        ArrayList<KeyValue> ret = new ArrayList<KeyValue>();
        for (int i = 0; i < args.length; i += 2)
            ret.add(new KeyValue(new Column(tableName, args[i]), args[i+1]));
        return ret;
    }

    public ArrayList<String> getAllKeyValueValues(KeyValueRow row) {
        ArrayList<String> ret = new ArrayList<String>();
        for (Column column : row.getColumns())
            ret.addAll(row.getValues(column));
        return ret;
    }
}
