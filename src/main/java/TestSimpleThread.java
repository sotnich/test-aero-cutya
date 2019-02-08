import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSimpleThread extends Thread {

    private final Random m_rnd = new Random(System.currentTimeMillis());
    private AerospikeClient m_client;
    private String m_namespace;

    private long m_current_rows_per_sec = 0;        // Тукущая производительность строк в секунду
    private long m_total_secs = 0;                  // Общее кол-во отработанных секунд
    private long m_total_rows = 0;                  // Общее кол-во обработанных строк

    private final int m_intervalMillis = 1000;      // Интеравал замера в миллисекундах

    private static AtomicInteger m_theadNumAtimic = new AtomicInteger(0);

    private int m_theadNum;

    public TestSimpleThread(AerospikeClient client, String namespace) {
        m_client = client;
        m_namespace = namespace;
        m_theadNum = m_theadNumAtimic.incrementAndGet();
    }

    public void run() {
        for (int i = 0; i < 10; i++) {
            runOnce(m_client, m_namespace);
        }
    }

    public void runOnce(AerospikeClient client, String namespace) {
        long beforeMillis = System.currentTimeMillis();

        long n = 0;
        long currMillis = beforeMillis;
        while (currMillis < beforeMillis + m_intervalMillis) {
            n = n + execute(client, namespace);
            currMillis = System.currentTimeMillis();
        }

        m_current_rows_per_sec = Math.round(n/((currMillis-beforeMillis)/1000));
        m_total_secs += currMillis-beforeMillis;
        m_total_rows += n;

        System.out.println("[" + getName() + "][" + formatDate(currMillis) + "]: " + m_current_rows_per_sec + " per sec" );
    }

    private long execute(AerospikeClient client, String namespace) {
        Key key = new Key(namespace, "demo", m_rnd.nextLong());
        Bin bin1 = new Bin("bin1", m_rnd.nextLong());
        client.put(null, key, bin1);
        return 1;
    }

    private String formatDate(long millis) {
        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss");
        Date date = new Date(millis);
        return formatter.format(date);
    }

    public long getCurrentRowsPerSec() {
        return m_current_rows_per_sec;
    }
}
