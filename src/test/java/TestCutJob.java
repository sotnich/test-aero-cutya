import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.*;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;

import java.util.*;

public class TestCutJob extends BaseTest {

    private CutEngine m_engine;

    public TestCutJob() {
        m_engine = new CutEngine(m_client, m_namespace, Utils.loadRelationsFromArray(new String [][] {
                {"EMART 1 LOAD ACCOUNT INSTALLMENT A", "prod_dds.installment", "installment_rk",  "prod_dds.installment",                 "installment_rk", "group"},
                {"EMART 1 LOAD ACCOUNT INSTALLMENT A", "prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng",      "account_rk", "left"},
                {"EMART 1 LOAD ACCOUNT INSTALLMENT A", "prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng_bal",  "account_rk", "left"}
        }));

        deleteTable("prod_dds.installment");
        deleteTable("prod_dds.financial_account_chng");
        deleteTable("prod_dds.financial_account_chng_bal");

        m_engine.initTable("prod_dds.installment", new String [][] {
                {"account_rk", "installment_rk"},
                {"100", "210"},
                {"101", "201"},
                {"101", "202"},
                {"102", "201"},
                {"102", "202"},
                {"102", "203"},
                {"103", "204"},
        });
    }

    @Test
    public void testCreation() {

        CutJob job = new CutJob(m_client, m_namespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", m_engine);

        // Проверяем что создалась ровно одна таблица для трансляции ключей
        Assert.assertTrue(job.getCutLinkTables().size() == 1);
        Assert.assertEquals(job.getCutLinkTables().get(0).getTable().getTableName() , "prod_dds.installment");
    }

    @Test
    public void testPutNextTableInc_one_right_table() {

        CutJob job = new CutJob(m_client, m_namespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", m_engine);

        TableValues vals = Utils.getTableValues("prod_dds.financial_account_chng", new String [][] {
                {"account_rk"},
                {"111"},
                {"101"},
                {"101"}
        });

        job.putTable(vals);

        ArrayList<String> etalonAccs = new ArrayList<String>(Arrays.asList("101", "102"));
        ArrayList<String> etalonInts = new ArrayList<String>(Arrays.asList("201", "202", "203"));
        ArrayList<String> keysAccs = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "account_rk"))));
        ArrayList<String> keysInst = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "installment_rk"))));

        assertArrays(etalonAccs, keysAccs);
        assertArrays(etalonInts, keysInst);

        deleteTable("prod_dds.installment");
    }

    @Test
    public void testPutNextTableInc_two_right_tables() {

        CutJob job = new CutJob(m_client, m_namespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", m_engine);

        TableValues vals1 = Utils.getTableValues("prod_dds.financial_account_chng", new String [][] {
                {"account_rk"},
                {"111"},
                {"101"},
                {"101"}
        });

        TableValues vals2 = Utils.getTableValues("prod_dds.financial_account_chng_bal", new String [][] {
                {"account_rk"},
                {"112"},
                {"101"},
                {"101"},
                {"103"},
        });

        job.putTable(vals1);
        job.putTable(vals2);

        ArrayList<String> etalonAccs = new ArrayList<String>(Arrays.asList("101", "102", "103"));
        ArrayList<String> etalonInts = new ArrayList<String>(Arrays.asList("201", "202", "203", "204"));
        ArrayList<String> keysAccs = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "account_rk"))));
        ArrayList<String> keysInst = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "installment_rk"))));

        assertArrays(etalonAccs, keysAccs);
        assertArrays(etalonInts, keysInst);

        deleteTable("prod_dds.installment");
    }

    @Test
    public void testPutNextTableInc_one_left_table() {

        String tableName = "prod_dds.installment";
        CutJob job = new CutJob(m_client, m_namespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", m_engine);

        TableValues vals = Utils.getTableValues("prod_dds.financial_account_chng", new String [][] {
                {"account_rk", "installment_rk"},
                {"108", "220"},
                {"109", "230"},
        });

        job.putTable(vals);

        ArrayList<String> etalonAccs = new ArrayList<String>(Arrays.asList("108", "109"));
        ArrayList<String> etalonInts = new ArrayList<String>(Arrays.asList("220", "230"));
        ArrayList<String> keysAccs = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "account_rk"))));
        ArrayList<String> keysInst = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "installment_rk"))));

        assertArrays(etalonAccs, keysAccs);
        assertArrays(etalonInts, keysInst);

        deleteTable("prod_dds.installment");
    }

    @Test
    public void testPutNextTableInc_one_left_table_with_revers() {

        String tableName = "prod_dds.installment";
        CutJob job = new CutJob(m_client, m_namespace, "EMART 1 LOAD ACCOUNT INSTALLMENT A", m_engine);

        TableValues vals = Utils.getTableValues("prod_dds.financial_account_chng", new String [][] {
                {"account_rk", "installment_rk"},
                {"108", "201"},
        });

        job.putTable(vals);

        ArrayList<String> etalonAccs = new ArrayList<String>(Arrays.asList("108", "101", "102"));
        ArrayList<String> etalonInts = new ArrayList<String>(Arrays.asList("201", "202", "203"));
        ArrayList<String> keysAccs = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "account_rk"))));
        ArrayList<String> keysInst = new ArrayList<String>(job.getKeys().getValues((new Column("prod_dds.installment", "installment_rk"))));

        assertArrays(etalonAccs, keysAccs);
        assertArrays(etalonInts, keysInst);

        deleteTable("prod_dds.installment");
    }
}
