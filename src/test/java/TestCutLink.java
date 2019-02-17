import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.CutLinkTable;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

import java.util.Arrays;
import java.util.List;

public class TestCutLink extends BaseTest {

    @Test
    public void testLookup1() {

        String tableName = "prod_dds.installment";

        deleteTable(tableName);
        CutLinkTable installment = new CutLinkTable(m_client, m_testNamespace, new Table(tableName, Arrays.asList("account_rk", "installment_rk")));

        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "210"));
        installment.addRow(getKeyValues(tableName,"account_rk", "101", "installment_rk", "201"));
        installment.addRow(getKeyValues(tableName,"account_rk", "101", "installment_rk", "202"));
        installment.addRow(getKeyValues(tableName,"account_rk", "102", "installment_rk", "201"));
        installment.addRow(getKeyValues(tableName,"account_rk", "102", "installment_rk", "202"));
        installment.addRow(getKeyValues(tableName,"account_rk", "102", "installment_rk", "203"));
        installment.addRow(getKeyValues(tableName,"account_rk", "103", "installment_rk", "204"));

        ColumnsValues res = installment.lookup(new Column(tableName, "account_rk"),
                Arrays.asList("101", "102", "103", "104"));
        List<String> etalon = Arrays.asList("201", "202", "203", "204");

        Assert.assertEquals(res.getColumns().size(), 1);
        assertArrays(getAllKeyValueValues(res), etalon);
    }
}
