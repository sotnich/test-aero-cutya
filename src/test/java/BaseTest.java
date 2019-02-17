import com.aerospike.client.*;
import org.junit.Assert;
import tinkoff.dwh.cut.data.KeyValue;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

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

    public ArrayList<String> getAllKeyValueValues(ColumnsValues row) {
        ArrayList<String> ret = new ArrayList<String>();
        for (Column column : row.getColumns())
            ret.addAll(row.getValues(column));
        return ret;
    }

    public TableValues getTableValues(String tableName, String [][] vals) {
        Table table = new Table(tableName);
        for (int i = 0; i < vals[0].length; i++)
            table.addColumn(new Column(tableName, vals[0][i]));

        TableValues ret = new TableValues(table);
        for (int j = 1; j < vals.length; j++ )
                ret.addRow(vals[j]);

        return ret;
    }

    public void deleteTable(String tableName) {
        m_client.scanAll(null, m_testNamespace, tableName, new ScanCallback() {
            public void scanCallback(Key key, Record record) throws AerospikeException {
                m_client.delete(null, key);
            }
        });
    }
}
