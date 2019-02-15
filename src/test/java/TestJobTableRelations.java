import org.junit.Assert;
import org.junit.Test;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.JobTableRelations;
import tinkoff.dwh.cut.meta.SingleKey;
import tinkoff.dwh.cut.meta.TableRelation;

import java.util.ArrayList;

public class TestJobTableRelations {

    @Test
    public void testInitSingleKeys1() {

        JobTableRelations jobRelations = new JobTableRelations(new String [][] {
                {"prod_dds.installment", "installment_rk",  "prod_dds.installment",                 "installment_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng",      "account_rk"},
                {"prod_dds.installment", "account_rk",      "prod_dds.financial_account_chng_bal",  "account_rk"}
        });

        SingleKey etalonKey1 = new SingleKey();
        etalonKey1.addColumn(new Column("prod_dds.installment", "installment_rk"));

        SingleKey etalonKey2 = new SingleKey();
        etalonKey2.addColumn(new Column("prod_dds.installment", "account_rk"));
        etalonKey2.addColumn(new Column("prod_dds.financial_account_chng",      "account_rk"));
        etalonKey2.addColumn(new Column("prod_dds.financial_account_chng_bal",  "account_rk"));

        ArrayList<SingleKey> etalon = new ArrayList<SingleKey>();
        etalon.add(etalonKey1);
        etalon.add(etalonKey2);

        Assert.assertEquals(jobRelations.getSingleKeys().size(), etalon.size());
        Assert.assertTrue(jobRelations.getSingleKeys().containsAll(etalon) && etalon.containsAll(jobRelations.getSingleKeys()));
    }

    @Test
    public void testInitSingleKeys2() {

        JobTableRelations jobRelations = new JobTableRelations(new String [][] {
                {"prod_odd.cre_report_summary",         "hjid",             "prod_ods_credb.sf_singleformattype",   "main"},
                {"prod_ods_credb.sf_singleformattype",  "hjid",             "prod_ods_credb.sf_loanstype",          "hjid"},
                {"prod_odd.siebel_opportunity",         "integration_id",   "prod_odd.siebel_opportunity",          "integration_id"},
                {"prod_odd.cre_report_summary",         "application_id",   "prod_odd.siebel_opportunity",          "integration_id"},
        });

        SingleKey etalonKey1 = new SingleKey();
        etalonKey1.addColumn(new Column("prod_odd.cre_report_summary", "hjid"));
        etalonKey1.addColumn(new Column("prod_ods_credb.sf_singleformattype", "main"));

        SingleKey etalonKey2 = new SingleKey();
        etalonKey2.addColumn(new Column("prod_ods_credb.sf_singleformattype", "hjid"));
        etalonKey2.addColumn(new Column("prod_ods_credb.sf_loanstype",      "hjid"));

        SingleKey etalonKey3 = new SingleKey();
        etalonKey3.addColumn(new Column("prod_odd.siebel_opportunity", "integration_id"));
        etalonKey3.addColumn(new Column("prod_odd.cre_report_summary",      "application_id"));

        ArrayList<SingleKey> etalon = new ArrayList<SingleKey>();
        etalon.add(etalonKey1);
        etalon.add(etalonKey2);
        etalon.add(etalonKey3);

        Assert.assertEquals(jobRelations.getSingleKeys().size(), etalon.size());
        Assert.assertTrue(jobRelations.getSingleKeys().containsAll(etalon) && etalon.containsAll(jobRelations.getSingleKeys()));
    }
}
