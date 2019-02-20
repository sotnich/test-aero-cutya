import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.CutLinkTable;
import tinkoff.dwh.cut.Utils;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestCutLink extends BaseTest {

    @Test
    public void testLookup1() {

        String tableName = "prod_dds.installment";

        deleteTable(tableName);
        CutLinkTable installment = new CutLinkTable(m_client, m_namespace, new Table(tableName, Arrays.asList("account_rk", "installment_rk")));

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

        deleteTable(tableName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSimpleAdd() {
        String tableName = "prod_dds.installment";
        deleteTable(tableName);

        CutLinkTable installment = new CutLinkTable(m_client, m_namespace, new Table(tableName, Arrays.asList("account_rk", "installment_rk")));

        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "210"));

        Assert.assertEquals(2, Utils.getSetSize(tableName, m_client, m_namespace));

        Record accnRec = m_client.get(null, new Key(m_namespace, tableName, "ACCN100"));
        Record instRec = m_client.get(null, new Key(m_namespace, tableName, "INST210"));

        Assert.assertEquals(accnRec.bins.size(), 1);
        Assert.assertEquals(instRec.bins.size(), 1);

        assertArrays((ArrayList<String>) accnRec.getValue("installment_rk"), "210");
        assertArrays((ArrayList<String>) instRec.getValue("account_rk"), "100");

        deleteTable(tableName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddArray() {
        String tableName = "prod_dds.installment";
        deleteTable(tableName);

        CutLinkTable installment = new CutLinkTable(m_client, m_namespace, new Table(tableName, Arrays.asList("account_rk", "installment_rk")));

        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "210"));
        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "211"));
        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "212"));

        Assert.assertEquals(4, Utils.getSetSize(tableName, m_client, m_namespace));

        Record accnRec = m_client.get(null, new Key(m_namespace, tableName, "ACCN100"));
        Record instRec1 = m_client.get(null, new Key(m_namespace, tableName, "INST210"));
        Record instRec2 = m_client.get(null, new Key(m_namespace, tableName, "INST211"));
        Record instRec3 = m_client.get(null, new Key(m_namespace, tableName, "INST212"));

        Assert.assertEquals(accnRec.bins.size(), 1);
        Assert.assertEquals(instRec1.bins.size(), 1);
        Assert.assertEquals(instRec2.bins.size(), 1);
        Assert.assertEquals(instRec3.bins.size(), 1);

        assertArrays((ArrayList<String>) accnRec.getValue("installment_rk"), "210", "211", "212");
        assertArrays((ArrayList<String>) instRec1.getValue("account_rk"), "100");
        assertArrays((ArrayList<String>) instRec2.getValue("account_rk"), "100");
        assertArrays((ArrayList<String>) instRec3.getValue("account_rk"), "100");

        deleteTable(tableName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddCrossArrays() {
        String tableName = "prod_dds.installment";
        deleteTable(tableName);

        CutLinkTable installment = new CutLinkTable(m_client, m_namespace, new Table(tableName, Arrays.asList("account_rk", "installment_rk")));

        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "210"));
        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "211"));
        installment.addRow(getKeyValues(tableName,"account_rk", "101", "installment_rk", "210"));
        installment.addRow(getKeyValues(tableName,"account_rk", "101", "installment_rk", "211"));

        Assert.assertEquals(4, Utils.getSetSize(tableName, m_client, m_namespace));

        Record accnRec1 = m_client.get(null, new Key(m_namespace, tableName, "ACCN100"));
        Record accnRec2 = m_client.get(null, new Key(m_namespace, tableName, "ACCN101"));
        Record instRec1 = m_client.get(null, new Key(m_namespace, tableName, "INST210"));
        Record instRec2 = m_client.get(null, new Key(m_namespace, tableName, "INST211"));

        Assert.assertEquals(accnRec1.bins.size(), 1);
        Assert.assertEquals(accnRec2.bins.size(), 1);
        Assert.assertEquals(instRec1.bins.size(), 1);
        Assert.assertEquals(instRec2.bins.size(), 1);

        assertArrays((ArrayList<String>) accnRec1.getValue("installment_rk"), "210", "211");
        assertArrays((ArrayList<String>) accnRec2.getValue("installment_rk"), "210", "211");
        assertArrays((ArrayList<String>) instRec1.getValue("account_rk"), "100", "101");
        assertArrays((ArrayList<String>) instRec2.getValue("account_rk"), "100", "101");

        deleteTable(tableName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddWithEmpty() {
        String tableName = "prod_dds.installment";
        deleteTable(tableName);

        CutLinkTable installment = new CutLinkTable(m_client, m_namespace, new Table(tableName, Arrays.asList("account_rk", "installment_rk")));

        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", "210"));
        installment.addRow(getKeyValues(tableName,"account_rk", "100", "installment_rk", ""));
        installment.addRow(getKeyValues(tableName,"account_rk", "", "installment_rk", "210"));
        installment.addRow(getKeyValues(tableName,"account_rk", "", "installment_rk", "" + ""));

        Assert.assertEquals(2, Utils.getSetSize(tableName, m_client, m_namespace));

        Record accnRec = m_client.get(null, new Key(m_namespace, tableName, "ACCN100"));
        Record instRec = m_client.get(null, new Key(m_namespace, tableName, "INST210"));

        Assert.assertEquals(accnRec.bins.size(), 1);
        Assert.assertEquals(instRec.bins.size(), 1);

        assertArrays((ArrayList<String>) accnRec.getValue("installment_rk"), "210");
        assertArrays((ArrayList<String>) instRec.getValue("account_rk"), "100");

        deleteTable(tableName);
    }
}
