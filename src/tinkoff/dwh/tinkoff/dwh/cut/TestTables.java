package tinkoff.dwh.cut;

import com.aerospike.client.AerospikeClient;

import java.util.ArrayList;

public class TestTables {

    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("178.128.134.224", 3000);

        testLookup(client);

        client.close();
    }

    public static void testLookup(AerospikeClient client) {
        CutTable installment = new CutTable("installment", client, "bar");

        installment.putSecondaryKey("account_rk", "101", "installment_rk", "201");
        installment.putSecondaryKey("account_rk", "101", "installment_rk", "202");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "201");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "202");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "203");
        installment.putSecondaryKey("account_rk", "103", "installment_rk", "204");
        installment.putSecondaryKey("account_rk", "100", "installment_rk", "210");

        ArrayList<String> prmKeys = new ArrayList<String>();
        prmKeys.add("101");
        prmKeys.add("102");
        prmKeys.add("103");
        prmKeys.add("104");
        ArrayList<String> res = installment.lookup("account_rk", prmKeys, "installment_rk");
        System.out.println(res);
    }

    public static void test(AerospikeClient client) {
        CutTable installment = new CutTable("installment", client, "bar");
        installment.putSecondaryKey("account_rk", "234", "installment_rk", "455");

        ArrayList<String> res = installment.getSecondaryKeys("account_rk", "123", "installment_rk");
        System.out.println(res);
    }
}
