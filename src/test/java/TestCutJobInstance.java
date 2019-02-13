import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.CutEngine;
import tinkoff.dwh.cut.CutJobInstance;
import tinkoff.dwh.cut.CutLinkTable;
import tinkoff.dwh.cut.TableRelation;

import java.util.*;

public class TestCutJobInstance extends BaseTest {

    CutEngine m_cutEngine;
    private String m_jobName = "EMART 1 LOAD ACCOUNT INSTALLMENT A";

    public TestCutJobInstance() {
        Map<String, List<String>> tableKeyMetadata = new HashMap<String, List<String>>();
        Map<String, List<TableRelation>> jobTableRelations = new HashMap<String, List<TableRelation>>();

        String tableName = "prod_dds.installment";
        List<String> keys = new ArrayList<String>();
        keys.add("account_rk");
        keys.add("installment_rk");
        tableKeyMetadata.put(tableName, keys);

        tableName = "prod_dds.financial_account_chng";
        keys = new ArrayList<String>();
        keys.add("account_rk");
        tableKeyMetadata.put(tableName, keys);

        tableName = "prod_dds.financial_account_chng_bal";
        keys = new ArrayList<String>();
        keys.add("account_rk");
        tableKeyMetadata.put(tableName, keys);

        List<TableRelation> relations = new ArrayList<TableRelation>();
        relations.add(new TableRelation("prod_dds.installment", "installment_rk", "prod_dds.installment", "installment_rk"));
        relations.add(new TableRelation("prod_dds.installment", "account_rk", "prod_dds.financial_account_chng", "account_rk"));
        relations.add(new TableRelation("prod_dds.installment", "account_rk", "prod_dds.financial_account_chng_bal", "account_rk"));
        jobTableRelations.put(m_jobName, relations);

        m_cutEngine = new CutEngine(tableKeyMetadata, jobTableRelations);
    }

    @Test
    public void testCreation() {
        CutJobInstance job = new CutJobInstance(m_client, m_testNamespace, m_jobName, m_cutEngine);

        // Проверяем что создалась ровно одна таблица для трансляции ключей
        Assert.assertTrue(job.getCutLinkTables().size() == 1);
        Assert.assertEquals(job.getCutLinkTables().get(0).getTableName() , "prod_dds.installment");

        // Проверим что у таблицы трансляции ключей ровно правильные колонки
        CutLinkTable linkTable = job.getCutLinkTables().get(0);
        ArrayList<String> resColumns = linkTable.getColumnNames();
        ArrayList<String> etalonColumns = new ArrayList<String>();
        etalonColumns.add("account_rk");
        etalonColumns.add("installment_rk");
        Assert.assertTrue(resColumns.size() == etalonColumns.size() && resColumns.containsAll(etalonColumns) && etalonColumns.containsAll(resColumns));
    }

    @Test
    public void testPutNextTableInc_financial_account_chng() {
        CutJobInstance job = new CutJobInstance(m_client, m_testNamespace, m_jobName, m_cutEngine);
        CutLinkTable installment = job.getCutLinkTables().get(0);

        installment.putSecondaryKey("account_rk", "100", "installment_rk", "210");
        installment.putSecondaryKey("account_rk", "101", "installment_rk", "201");
        installment.putSecondaryKey("account_rk", "101", "installment_rk", "202");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "201");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "202");
        installment.putSecondaryKey("account_rk", "102", "installment_rk", "203");
        installment.putSecondaryKey("account_rk", "103", "installment_rk", "204");

        ArrayList<String> columnNames = new ArrayList<String>();
        columnNames.add("account_rk");
        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
        ArrayList<String> row01 = new ArrayList<String>();
        row01.add("111");
        rows.add(row01);
        ArrayList<String> row02 = new ArrayList<String>();
        row02.add("101");
        rows.add(row02);
        ArrayList<String> row03 = new ArrayList<String>();
        row03.add("101");
        rows.add(row03);

        job.putNextTableInc("prod_dds.financial_account_chng", columnNames, rows);

        HashMap<String, Set<String>> keys = job.getKeys();

        Assert.assertEquals(keys.size(), 2);     // Два массива ключей
        Assert.assertTrue(keys.containsKey("account_rk"));
        Assert.assertTrue(keys.containsKey("installment_rk"));

        String [] etalonAccs = {"111", "101"};
        String [] etalonInts = {"201", "202"};

        Assert.assertTrue(keys.get("account_rk").contains(etalonAccs[0]));
        Assert.assertTrue(keys.get("account_rk").contains(etalonAccs[1]));

        Assert.assertTrue(keys.get("installment_rk").contains(etalonInts[0]));
        Assert.assertTrue(keys.get("installment_rk").contains(etalonInts[1]));
    }
}
