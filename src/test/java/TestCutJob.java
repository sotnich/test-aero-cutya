import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.*;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.JobTableRelations;

import java.util.*;

public class TestCutJob extends BaseTest {

    @Test
    public void testCreation() {

        JobTableRelations relations = new JobTableRelations(new String [][] {
                {"prod_dds.installment", "installment_rk",  "prod_dds.installment",                 "installment_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng",      "account_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng_bal",  "account_rk"}
        });
        CutJob job = new CutJob(m_client, m_testNamespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", new CutEngine(relations));

        // Проверяем что создалась ровно одна таблица для трансляции ключей
        Assert.assertTrue(job.getCutLinkTables().size() == 1);
        Assert.assertEquals(job.getCutLinkTables().get(0).getTable().getTableName() , "prod_dds.installment");
    }

    @Test
    public void testPutNextTableInc_financial_account_chng() {

        JobTableRelations relations = new JobTableRelations(new String [][] {
                {"prod_dds.installment", "installment_rk",  "prod_dds.installment",                 "installment_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng",      "account_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng_bal",  "account_rk"}
        });
        CutJob job = new CutJob(m_client, m_testNamespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", new CutEngine(relations));

        CutLinkTable installment = job.getCutLinkTables().get(0);

        installment.addRow(getKeyValues("prod_dds.installment","account_rk", "100", "installment_rk", "210"));
        installment.addRow(getKeyValues("prod_dds.installment","account_rk", "101", "installment_rk", "201"));
        installment.addRow(getKeyValues("prod_dds.installment","account_rk", "101", "installment_rk", "202"));
        installment.addRow(getKeyValues("prod_dds.installment","account_rk", "102", "installment_rk", "201"));
        installment.addRow(getKeyValues("prod_dds.installment","account_rk", "102", "installment_rk", "202"));
        installment.addRow(getKeyValues("prod_dds.installment","account_rk", "102", "installment_rk", "203"));
        installment.addRow(getKeyValues("prod_dds.installment","account_rk", "103", "installment_rk", "204"));

        TableValues vals = getTableValues("prod_dds.financial_account_chng", new String [][] {
                {"account_rk"},
                {"111"},
                {"101"},
                {"101"}
        });

        job.putTable(vals);

        ArrayList<String> etalonAccs = new ArrayList<String>(Arrays.asList("111", "101", "102"));
        ArrayList<String> etalonInts = new ArrayList<String>(Arrays.asList("201", "202", "203"));
        ArrayList<String> keysAccs = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "account_rk"))));
        ArrayList<String> keysInst = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "installment_rk"))));

        assertArrays(etalonAccs, keysAccs);
        assertArrays(etalonInts, keysInst);
    }
}