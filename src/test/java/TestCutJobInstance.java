import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader;
import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.*;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.JobTableRelations;
import tinkoff.dwh.cut.meta.SingleKey;
import tinkoff.dwh.cut.meta.TableRelation;

import java.util.*;

public class TestCutJobInstance extends BaseTest {

    @Test
    public void testCreation() {

        String textRelations [][] = {
                {"prod_dds.installment", "installment_rk",  "prod_dds.installment",                 "installment_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng",      "account_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng_bal",  "account_rk"}
        };
        JobTableRelations relations = new JobTableRelations(textRelations);
        CutJobInstance job = new CutJobInstance(m_client, m_testNamespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", new CutEngine(relations));

        // Проверяем что создалась ровно одна таблица для трансляции ключей
        Assert.assertTrue(job.getCutLinkTables().size() == 1);
        Assert.assertEquals(job.getCutLinkTables().get(0).getTableName() , "prod_dds.installment");
    }

    @Test
    public void testPutNextTableInc_financial_account_chng() {

        String textRelations [][] = {
                {"prod_dds.installment", "installment_rk",  "prod_dds.installment",                 "installment_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng",      "account_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng_bal",  "account_rk"}
        };
        JobTableRelations relations = new JobTableRelations(textRelations);
        CutJobInstance job = new CutJobInstance(m_client, m_testNamespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", new CutEngine(relations));

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

        ArrayList<String> etalonAccs = new ArrayList<String>(Arrays.asList("111", "101"));
        ArrayList<String> etalonInts = new ArrayList<String>(Arrays.asList("201", "202"));
        ArrayList<String> keysAccs = new ArrayList<String>(job.getKeys(new Column("prod_dds.installment", "account_rk")));
        ArrayList<String> keysInst = new ArrayList<String>(job.getKeys(new Column("prod_dds.installment", "installment_rk")));

        assertArrays(etalonAccs, keysAccs);
        assertArrays(etalonInts, keysInst);
    }
}
