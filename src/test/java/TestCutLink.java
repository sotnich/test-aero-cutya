import com.aerospike.client.AerospikeClient;
import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.CutLinkTable;

import java.util.ArrayList;

public class TestCutLink extends BaseTest {

    private CutLinkTable m_installment;

    public TestCutLink() {

        ArrayList<String> installmentColumns = new ArrayList<String>();
        installmentColumns.add("account_rk");
        installmentColumns.add("installment_rk");
        m_installment = new CutLinkTable("installment", m_client, m_testNamespace, installmentColumns);


    }

    @Test
    public void testGetSecondaryKeys() {
        m_installment.putSecondaryKey("account_rk", "100", "installment_rk", "210");
        m_installment.putSecondaryKey("account_rk", "101", "installment_rk", "201");
        m_installment.putSecondaryKey("account_rk", "101", "installment_rk", "202");
        m_installment.putSecondaryKey("account_rk", "102", "installment_rk", "201");
        m_installment.putSecondaryKey("account_rk", "102", "installment_rk", "202");
        m_installment.putSecondaryKey("account_rk", "102", "installment_rk", "203");
        m_installment.putSecondaryKey("account_rk", "103", "installment_rk", "204");

        ArrayList<String> prmKeys = new ArrayList<String>();
        prmKeys.add("101");
        prmKeys.add("102");
        prmKeys.add("103");
        prmKeys.add("104");

        ArrayList<String> res = m_installment.getSecondaryKeys("account_rk", prmKeys, "installment_rk");
        ArrayList<String> etalon = new ArrayList<String>();
        etalon.add("201");
        etalon.add("202");
        etalon.add("203");
        etalon.add("204");

        Assert.assertTrue(res.size() == etalon.size() && res.containsAll(etalon) && etalon.containsAll(res));
    }
}
