package tinkoff.dwh.cut;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import tinkoff.dwh.cut.data.KeyValue;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class Utils {
    public static TableValues getTableValues(String tableName, String [][] vals) {
        Table table = new Table(tableName);
        for (int i = 0; i < vals[0].length; i++)
            table.addColumn(new Column(tableName, vals[0][i]));

        TableValues ret = new TableValues(table);
        for (int j = 1; j < vals.length; j++ )
            ret.addRow(vals[j]);

        return ret;
    }

    public static void loadFromCSV(CutLinkTable linkTable, String csvFile) {
        ArrayList<Column> columns = linkTable.getTable().getColumns();
        String line = "";
        String cvsSplitBy = ";";
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            br.readLine().split(cvsSplitBy);

            while ((line = br.readLine()) != null) {
                linkTable.addRow(KeyValue.fromArray(columns, line.split("\\;",-1)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteTable(String tableName, final AerospikeClient client, String namespace) {
        final ScanPolicy policy = new ScanPolicy();
        policy.concurrentNodes = true;
        policy.includeBinData = false;
        client.scanAll(policy, namespace, tableName, new ScanCallback() {
            public void scanCallback(Key key, Record record) throws AerospikeException {
                client.delete(null, key);
            }
        });
    }

    private static class ScanCnt implements ScanCallback {
        private int recordCount;

        public int runTest(AerospikeClient client, String namespace, String tableName) throws AerospikeException {
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = false;
            policy.includeBinData = false;

            client.scanAll(policy, namespace, tableName, this);
            return recordCount;
        }

        public void scanCallback(Key key, Record record) {
            recordCount++;
        }
    }

    private static class ScanProfile implements ScanCallback {
        private int recordCount = 0;
        private HashMap<Long, Long> m_cntCnt = new HashMap<Long, Long>();

        public void runTest(AerospikeClient client, String namespace, String tableName) throws AerospikeException {
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = false;
            policy.includeBinData = true;

            client.scanAll(policy, namespace, tableName, this);
        }

        public void scanCallback(Key key, Record record) {
            recordCount++;

            long cntKey = 0L;
            for (String binName : record.bins.keySet()) {
                ArrayList<String> vals = (ArrayList<String>) record.getValue(binName);
                cntKey += vals.size();
            }

            if (!m_cntCnt.containsKey(cntKey))
                m_cntCnt.put(cntKey, 0L);
            m_cntCnt.put(cntKey, m_cntCnt.get(cntKey) + 1);

            recordCount++;
        }

        public void printStat() {
            System.out.println("record cnt: " + recordCount);
            for (Long key : m_cntCnt.keySet())
                System.out.println(" " + key + " values in key cnt: " + m_cntCnt.get(key));
        }
    }

    public static int getSetSize(String tableName, final AerospikeClient client, String namespace) {
        ScanCnt scan = new ScanCnt();
        return scan.runTest(client, namespace, tableName);
    }

    public static void printSetProfile(String tableName, final AerospikeClient client, String namespace) {
        ScanProfile scan = new ScanProfile();
        scan.runTest(client, namespace, tableName);
        scan.printStat();
    }

    private static long m_beforeTime;
    private static String m_method;

    public static void startStep(String method) {
        System.out.println("[" + method + "] - start");
        m_beforeTime = System.currentTimeMillis();
        m_method = method;
    }

    public static void finishStep() {
        long currentTime = System.currentTimeMillis();
        int seconds = Math.round((currentTime - m_beforeTime)/1000);
        long millis = (currentTime - m_beforeTime) - seconds * 1000;
        System.out.println("[" + m_method + "] - finish in " + seconds + " secs and " + millis + " millisecs");
    }
}
