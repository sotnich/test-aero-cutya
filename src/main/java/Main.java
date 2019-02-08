import com.aerospike.client.AerospikeClient;
import org.apache.commons.cli.*;

public class Main {

    private static String default_host = "localhost";
    private static String default_port = "3000";
    private static String default_namespace = "test";

    public static void main(String[] args) {

        Options options = new Options();

        Option ohost = new Option("h", "host", true, "aerospike host");
        options.addOption(ohost);

        Option oport = new Option("p", "port", true, "aerospike port");
        options.addOption(oport);

        Option onamespace = new Option("n", "namespace", true, "aerospike namespace");
        options.addOption(onamespace);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            String host = cmd.getOptionValue("host");
            String port = cmd.getOptionValue("output");
            String namespace = cmd.getOptionValue("namespace");

            if (host == null) {
                System.out.println("host is empty, use default: " + default_host);
                host = default_host;
            }
            if (port == null) {
                System.out.println("port is empty, use default: " + default_port);
                port = default_port;
            }
            if (namespace == null) {
                System.out.println("namespace is empty, use default: " + default_namespace);
                namespace = default_namespace;
            }

            AerospikeClient client = new AerospikeClient(host, Integer.parseInt(port));

            TestSimple test = new TestSimple();
            test.run(client, namespace);

            client.close();
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("aero-test", options);

            System.exit(1);
        }
    }


}
