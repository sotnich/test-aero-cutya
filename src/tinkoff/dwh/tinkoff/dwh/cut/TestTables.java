package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;

import java.util.ArrayList;

public class TestTables {

    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("178.128.134.224", 3000);
//        AerospikeClient client = new AerospikeClient("10.216.193.85", 3000);

        client.close();
    }
}
