import com.aerospike.client.AerospikeClient;
import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.CutLinkTable;

import java.util.ArrayList;
import java.util.Arrays;

public class TestCutLink extends BaseTest {

    @Test
    public void testGetSecondaryKeys() {

        CutLinkTable installment = new CutLinkTable(m_client, m_testNamespace, "installment");

        installment.putSecondaryKey("account_rk", "100", "installment_rk", "210");
        installment.putSecondaryKey("account_rk", "101", "installment_rk", "201");
        installment.putSecondaryKey("account_rk", "101", "installment_rk", "202");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "201");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "202");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "203");
        installment.putSecondaryKey("account_rk", "103", "installment_rk", "204");

        ArrayList<String> prmKeys = new ArrayList<String>(Arrays.asList("101", "102", "103", "104"));
        ArrayList<String> res = installment.getSecondaryKeys("account_rk", prmKeys, "installment_rk");
        ArrayList<String> etalon = new ArrayList<String>(Arrays.asList("201", "202", "203", "204"));

        assertArrays(res, etalon);
    }
}
