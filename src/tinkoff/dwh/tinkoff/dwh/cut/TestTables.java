package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;

import java.util.ArrayList;

public class TestTables {

    public static void main(String[] args) {
//        AerospikeClient client = new AerospikeClient("178.128.134.224", 3000);
        AerospikeClient client = new AerospikeClient("10.216.193.85", 3000);

        CutAeroTable installment = new CutAeroTable("installment", client, "test");
        installment.putSecondaryKey("account_rk", "123", "installment_rk", "455");

        ArrayList<String> res = installment.getSecondaryKeys("account_rk", "123", "installment_rk");
        System.out.println(res);

        client.close();
    }
}
