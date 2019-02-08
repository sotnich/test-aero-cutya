import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class TestSimple {

    private final Random m_rnd = new Random(System.currentTimeMillis());

    public void run(AerospikeClient client, String namespace) {
        for (int i = 0; i < 100; i++) {
            runOnce(client, namespace);
        }
    }

    public void runOnce(AerospikeClient client, String namespace) {
        long beforeMillis = System.currentTimeMillis();

        long n = 0;
        long currMillis = beforeMillis;
        while (currMillis < beforeMillis + 3000) {
            n = n + execute(client, namespace);
            currMillis = System.currentTimeMillis();
        }

        long n_per_sec = Math.round(n/((currMillis-beforeMillis)/1000));
        System.out.println(formatDate(currMillis) + ": " + n_per_sec + " per sec" );
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

}
