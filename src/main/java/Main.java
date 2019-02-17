import com.aerospike.client.AerospikeClient;
import tinkoff.dwh.cut.CutLinkTable;
import tinkoff.dwh.cut.Utils;
import tinkoff.dwh.cut.meta.Table;

public class Main {

    public static void main(String[] args) {

        String aerospikeHost = "localhost";
//        String aerospikeHost = "178.128.134.224";
        String aerospikeNamespace = "test2";
        AerospikeClient client = new AerospikeClient(aerospikeHost, 3000);

        String tableName = "prod_dds.installment";

        Utils.startStep("delete all rows from " + tableName);
        Utils.deleteTable(tableName, client, aerospikeNamespace);
        Utils.finishStep();

        CutLinkTable installment = new CutLinkTable(client, aerospikeNamespace, new Table(tableName, "account_rk", "installment_rk"));

        Utils.startStep("load data from CSV");
        Utils.loadFromCSV(installment, "./data/INSTALLMENT.csv");
        Utils.finishStep();

        Utils.printSetProfile(tableName, client, aerospikeNamespace);

//        Utils.deleteTable(tableName, client, aerospikeNamespace);
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
