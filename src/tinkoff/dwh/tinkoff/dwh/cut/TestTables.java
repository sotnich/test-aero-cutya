package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;

import java.util.ArrayList;

public class TestTables {

    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("178.128.134.224", 3000);
//        AerospikeClient client = new AerospikeClient("10.216.193.85", 3000);

        client.close();
    }


    public static void test(AerospikeClient client) {
        CutLinkTable installment = new CutLinkTable("installment", client, "bar", null);
        installment.putSecondaryKey("account_rk", "234", "installment_rk", "455");

        ArrayList<String> res = installment.getSecondaryKeys("account_rk", "123", "installment_rk");
        System.out.println(res);

        client.close();
    }
}
