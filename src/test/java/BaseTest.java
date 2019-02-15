import com.aerospike.client.AerospikeClient;
import org.junit.Assert;

import java.util.ArrayList;

public class BaseTest {
//    static final String m_aerospikeHost = "178.128.134.224";
    static final String m_aerospikeHost = "10.216.193.85";
    static final String m_testNamespace = "test";
    AerospikeClient m_client;

    public BaseTest() {
        m_client = new AerospikeClient(m_aerospikeHost, 3000);
    }

    public void assertArrays(ArrayList<String> a1, ArrayList<String> a2) {
        Assert.assertTrue(a1.size() == a2.size() && a1.containsAll(a2) && a2.containsAll(a1));
    }

}
