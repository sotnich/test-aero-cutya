import com.aerospike.client.AerospikeClient;
import org.apache.commons.cli.*;
import tinkoff.dwh.cut.CutEngine;
import tinkoff.dwh.cut.CutJob;
import tinkoff.dwh.cut.CutLinkTable;
import tinkoff.dwh.cut.Utils;
import tinkoff.dwh.cut.data.ColumnValues;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.data.SingleKeysValues;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.SingleKey;
import tinkoff.dwh.cut.meta.Table;

import java.util.ArrayList;
import java.util.HashMap;

public class Main {

    private static String m_aerospikeHost = "localhost";
    private static String m_namespace = "test";
    private static String m_method = "init";
    private static String m_job = "";
    private static AerospikeClient m_client;
    private static CutEngine m_engine;

    public static void main(String[] args) {

        parseOptions(args);
        m_client = new AerospikeClient(m_aerospikeHost, 3000);

        m_engine = new CutEngine(m_client, m_namespace, Utils.loadRelationsFromCSV("./data/default_matrix_meta.csv"));

        if (m_method.equals("init"))
            init();
        else if (m_method.equals("cut"))
            cut();

        m_client.close();
    }

    private static void cut() {
        for (Table table : m_engine.getTables(m_job)) {
            cutTable(table);
        }

        System.out.println("");
        for (CutJob job : m_engine.getCuts()) {
            SingleKeysValues skv = job.getKeys();
            for (SingleKey sk : skv.getSingleKeys()) {
                System.out.println("[RESULT " + job.getJobName() + "] " + sk + ": " + skv.getValues(sk).size() );
            }
        }
    }

    private static void init() {
        for (Table table : m_engine.getTablesWithMoreThanOneColumn(m_job)) {
            initTable(table);
        }
    }

    private static ArrayList<String> loadCSV(String csvFile) {
        Utils.startStep("load csv " + csvFile);
        ArrayList<String> lines = Utils.loadCSV(csvFile);
        System.out.println("loaded " + lines.size() + " records");
        Utils.finishStep();
        return lines;
    }

    private static void cutTable(Table table) {
        String csvFile = "./data/" + table.getTableName() + ".inc.csv";
        ArrayList<String> lines = loadCSV(csvFile);

        Utils.startStep("add increment for " + table.getTableName());
        if (lines.get(0).split("\\;",-1).length == 1) {
            ColumnValues values = Utils.getSingleColumnValues(table, lines);
            m_engine.putColumnValues(table, values);
        }
        else {
            HashMap<Column, HashMap<String, ColumnsValues>> values = Utils.getColumnValues(table, lines);
            m_engine.putColumnsValues(table, values);
        }
        Utils.finishStep();
    }

    private static void initTable(Table table) {

        Utils.startStep("delete all rows from " + table.getTableName());
        Utils.deleteTable(table.getTableName(), m_client, m_namespace);
        Utils.finishStep();

        String csvName = "./data/" + table.getTableName() + ".csv";
        ArrayList<String> lines = loadCSV(csvName);

        Utils.startStep("builing values from csv lines for table " + table.getTableName());
        HashMap<Column, HashMap<String, ColumnsValues>> values = Utils.getColumnValues(table, lines);
        Utils.finishStep();

        CutLinkTable linkTable = new CutLinkTable(m_client, m_namespace, table);
        for (Column prmColumn : values.keySet()) {
            Utils.startStep("load data into table " + table.getTableName() + ", column " + prmColumn.getColumnName());
            linkTable.addValues(prmColumn, values.get(prmColumn));
            Utils.finishStep();
        }

        Utils.startStep("loading profile for table " + table.getTableName());
        Utils.printSetProfile(table.getTableName(), m_client, m_namespace);
        Utils.finishStep();
    }

    private static void parseOptions(String [] args) {

        String default_host = "localhost";

        Options options = new Options();

        options.addOption(new Option("h", "host", true, "aerospike host"));
        options.addOption(new Option("n", "namespace", true, "aerospike namespace"));
        options.addOption(new Option("m", "method", true, "method to execute"));
        options.addOption(new Option("j", "job", true, "job"));

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            String host = cmd.getOptionValue("host");
            String namespace = cmd.getOptionValue("namespace");
            String method = cmd.getOptionValue("method");
            String job = cmd.getOptionValue("job");


            if (host != null)
                m_aerospikeHost = host;
            if (namespace != null)
                m_namespace = namespace;
            if (method != null)
                m_method = method;
            if (job != null)
                m_job = job;
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("aero-test", options);
        }
    }

//    private static String default_host = "localhost";
//    private static String default_port = "3000";
//    private static String default_namespace = "test";
//    private static String default_threads = "2";
//    private static String default_method = "write";
//    private static String default_batch_size = "1000";
//    private static String default_seconds = "10";
//
//    public static void main(String[] args) {
//
//        Options options = new Options();
//
//        Option ohost = new Option("h", "host", true, "aerospike host");
//        options.addOption(ohost);
//
//        Option oport = new Option("p", "port", true, "aerospike port");
//        options.addOption(oport);
//
//        Option onamespace = new Option("n", "namespace", true, "aerospike namespace");
//        options.addOption(onamespace);
//
//        Option othread = new Option("t", "threads", true, "number of threads");
//        options.addOption(othread);
//
//        Option omethod = new Option("m", "method", true, "method (write|read|batchread|indexread|writetable)");
//        options.addOption(omethod);
//
//        Option otime = new Option("s", "seconds", true, "seconds to run");
//        options.addOption(otime);
//
//        Option obatch = new Option("bs", "batch_size", true, "batch size");
//        options.addOption(obatch);
//
//        Option oramndom_max = new Option("rm", "random_max", true, "max value for random numbers");
//        options.addOption(oramndom_max);
//
//        CommandLineParser parser = new DefaultParser();
//        HelpFormatter formatter = new HelpFormatter();
//        CommandLine cmd;
//
//        try {
//            cmd = parser.parse(options, args);
//
//            String host = cmd.getOptionValue("host");
//            String port = cmd.getOptionValue("output");
//            String namespace = cmd.getOptionValue("namespace");
//            String threads = cmd.getOptionValue("threads");
//            String method = cmd.getOptionValue("method");
//            String random_max = cmd.getOptionValue("random_max");
//            int random_max_n = -1;
//            String batch_zise = cmd.getOptionValue("batch_size");
//            String seconds = cmd.getOptionValue("seconds");
//
//            if (host == null) {
//                System.out.println("host is empty, use default: " + default_host);
//                host = default_host;
//            }
//            if (port == null) {
//                System.out.println("port is empty, use default: " + default_port);
//                port = default_port;
//            }
//            if (namespace == null) {
//                System.out.println("namespace is empty, use default: " + default_namespace);
//                namespace = default_namespace;
//            }
//            if (threads == null) {
//                System.out.println("threads is empty, use default: " + default_threads);
//                threads = default_threads;
//            }
//            if (method == null) {
//                System.out.println("method is empty, use default: " + default_method);
//                method = default_method;
//            }
//            if (!method.equals("read") && !method.equals("write") && !method.equals("batchread")
//                    && !method.equals("indexread") && !method.equals("writetable"))
//                throw new ParseException("invalid method option");
//            if (random_max != null) {
//                random_max_n = Integer.parseInt(random_max);
//            }
//            if (batch_zise == null) {
//                System.out.println("batch_size is empty, use default: " + default_batch_size);
//                batch_zise = default_batch_size;
//            }
//            if (seconds == null) {
//                System.out.println("seconds is empty, use default: " + default_seconds);
//                seconds = default_seconds;
//            }
//
//            AerospikeClient client = new AerospikeClient(host, Integer.parseInt(port));
//
//            List<TestSimpleThread> computeThreads = new ArrayList<TestSimpleThread>();
//            for (int i = 0; i <= Integer.parseInt(threads); i++) {
//                TestSimpleThread thread = new TestSimpleThread(client, namespace, method, random_max_n,
//                        Integer.parseInt(batch_zise), Integer.parseInt(seconds));
//                computeThreads.add(thread);
//                thread.start();
//            }
//
//            TotalStatThread statThread = new TotalStatThread(computeThreads);
//            statThread.start();
//
//            for (TestSimpleThread thread : computeThreads) {
//                try {
//                    thread.join();
//                }
//                catch (InterruptedException e) {
//                    System.out.println(e.getMessage());
//                }
//            }
//
//            try {
//                statThread.join();
//            }
//            catch (InterruptedException e) {
//                System.out.println(e.getMessage());
//            }
//
//            long totalRows = 0;
//            long totalHits = 0;
//            for (TestSimpleThread thread : computeThreads) {
//                totalRows += thread.getTotatRows();
//                totalHits += thread.getTotalHits();
//            }
//            System.out.println("");
//            System.out.println("Total rows: " + totalRows);
//            System.out.println("Total hits: " + totalHits);
//
//            client.close();
//        } catch (ParseException e) {
//            System.out.println(e.getMessage());
//            formatter.printHelp("aero-test", options);
//
//            System.exit(1);
//        }
//    }


}
