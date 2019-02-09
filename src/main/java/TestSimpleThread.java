import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSimpleThread extends Thread {

    private final Random m_rnd = new Random(System.currentTimeMillis());
    private AerospikeClient m_client;
    private String m_namespace;
    private String m_method;
    private int m_randomMax;
    private int m_batchSize;

    private long m_current_rows_per_sec = 0;        // Тукущая производительность строк в секунду
    private long m_total_rows = 0;                  // Общее кол-во обработанных строк
    private long m_total_hits = 0;                  // Общее кол-во попаданий при чтении

    private final int m_intervalMillis = 1000;      // Интеравал замера в миллисекундах

    TestSimpleThread(AerospikeClient client, String namespace, String method, int ramdomMax, int batchSize) {
        m_client = client;
        m_namespace = namespace;
        m_method = method;
        m_randomMax = ramdomMax;
        m_batchSize = batchSize;
    }

    public void run() {
        for (int i = 0; i < 10; i++) {
            runOnce();
        }
        m_current_rows_per_sec = 0;
    }

    private void runOnce() {
        long beforeMillis = System.currentTimeMillis();

        long n = 0;
        long currMillis = beforeMillis;
        while (currMillis < beforeMillis + m_intervalMillis) {
            n = n + execute();
            currMillis = System.currentTimeMillis();
        }

        m_current_rows_per_sec = Math.round(n/((currMillis-beforeMillis)/1000));
    }

    private long execute() {
        if (m_method.equals("write")) {
            return executeWrite();
        }
        else if (m_method.equals("read")) {
            return executeRead();
        }
        else if (m_method.equals("batchread")) {
            return executeBatchRead();
        }
        else return 0;
    }

    private long executeBatchRead() {
        Key[] keys = new Key[m_batchSize];

        for (int i = 0; i < m_batchSize; i++) {
            keys[i] = new Key(m_namespace, "demo", getRandom());
        }

        Record[] records = m_client.get(null, keys);

        for (Record record : records) {
            if (record != null)
                m_total_hits++;
        }

        m_total_rows += m_batchSize;
        return m_batchSize;
    }

    private long getRandom() {
        if (m_randomMax == -1) return m_rnd.nextLong();
        else return m_rnd.nextInt(m_randomMax);
    }

    private long executeRead() {
        Key key = new Key(m_namespace, "demo", getRandom());
        Record record = m_client.get(null, key);
        if (record != null)
            m_total_hits++;
        m_total_rows++;
        return 1;
    }

    private long executeWrite() {
        Key key = new Key(m_namespace, "demo", getRandom());
        Bin bin1 = new Bin("bin1", getRandom());
        m_client.put(null, key, bin1);
        m_total_rows++;
        return 1;
    }

    static String formatDate(long millis) {
        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss");
        Date date = new Date(millis);
        return formatter.format(date);
    }

    long getCurrentRowsPerSec() {
        return m_current_rows_per_sec;
    }

    long getTotalHits() {
        return m_total_hits;
    }

    long getTotatRows() {
        return m_total_rows;
    }
}
