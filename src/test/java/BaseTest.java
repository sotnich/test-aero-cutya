import com.aerospike.client.AerospikeClient;

public class BaseTest {
    static final String m_aerospikeHost = "178.128.134.224";
//    static final String m_aerospikeHost = "10.216.193.85";
    static final String m_testNamespace = "test";
    AerospikeClient m_client;

    public BaseTest() {
        m_client = new AerospikeClient(m_aerospikeHost, 3000);
    }
}
