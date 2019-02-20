import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.CutEngine;
import tinkoff.dwh.cut.Utils;
import tinkoff.dwh.cut.meta.Table;

import java.util.ArrayList;

public class TestCutEngine extends BaseTest {

    @Test
    public void testGetTablesWithMoreThanOneColumn() {
        CutEngine engine = new CutEngine(m_client, m_namespace, Utils.loadRelationsFromArray(new String [][] {
                {"EMART JOB 1", "prod_dds.installment", "installment_rk",       "prod_dds.installment",                 "installment_rk"},
                {"EMART JOB 1", "prod_dds.installment", "account_rk",           "prod_dds.financial_account_chng",      "account_rk"},
                {"EMART JOB 1", "prod_dds.installment", "account_rk",           "prod_dds.financial_account_chng_bal",  "account_rk"},
                {"EMART JOB 2", "prod_dds.installment", "parent_account_rk",    "prod_dds.financial_account_chng",      "account_rk"},
                {"EMART JOB 2", "prod_dds.installment", "account_rk",           "prod_dds.financial_account_chng_bal",  "parent_account_rk"}
        }));

        ArrayList<Table> res = engine.getTablesWithMoreThanOneColumn();

        Assert.assertEquals(1, res.size());
        Assert.assertEquals(new Table("prod_dds.installment", "installment_rk", "account_rk", "parent_account_rk"), res.get(0));
    }

    @Test
    public void testGetTables() {
        CutEngine engine = new CutEngine(m_client, m_namespace, Utils.loadRelationsFromArray(new String [][] {
                {"EMART JOB 1", "prod_dds.installment", "installment_rk",       "prod_dds.installment",                 "installment_rk"},
                {"EMART JOB 1", "prod_dds.installment", "account_rk",           "prod_dds.financial_account_chng",      "account_rk"},
                {"EMART JOB 1", "prod_dds.installment", "account_rk",           "prod_dds.financial_account_chng_bal",  "account_rk"},
                {"EMART JOB 2", "prod_dds.installment", "parent_account_rk",    "prod_dds.financial_account_chng",      "account_rk"},
                {"EMART JOB 2", "prod_dds.installment", "account_rk",           "prod_dds.financial_account_chng_bal",  "account_rk"}
        }));

        ArrayList<Table> res = engine.getTables();

        ArrayList<Table> etalon = new ArrayList<Table>();
        etalon.add(new Table("prod_dds.installment", "installment_rk", "account_rk", "parent_account_rk"));
        etalon.add(new Table("prod_dds.financial_account_chng",  "account_rk"));
        etalon.add(new Table("prod_dds.financial_account_chng_bal", "account_rk"));

        Assert.assertTrue(res.size() == etalon.size() && res.containsAll(etalon) && etalon.containsAll(res));
    }
}
