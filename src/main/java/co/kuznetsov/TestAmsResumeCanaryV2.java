package co.kuznetsov;


import com.mysql.cj.jdbc.Driver;
import picocli.CommandLine;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

@CommandLine.Command(name = "test-eni-move", mixinStandardHelpOptions = true,
        description = "Test ENI moves")
public class TestAmsResumeCanaryV2 implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-v", "--version"},
            description = "Engine version",
            defaultValue = "8.0.mysql_aurora.3.07.0")
    public String version;

    @CommandLine.Option(
            names = {"-sg", "--security-group"},
            description = "Security group",
            required = true)
    public String securityGroup;

    @CommandLine.Option(
            names = {"-db", "--database"},
            description = "Database name",
            defaultValue = "jff")
    public String database;

    @CommandLine.Option(
            names = {"-c", "--clusters"},
            description = "Number of clusters",
            defaultValue = "1")
    int clusters;

    @CommandLine.Option(
            names = {"-rds", "--rds-endpoint"},
            description = "RDS API endpoint",
            required = true)
    String rdsEndpoint;

    @CommandLine.Option(
            names = {"-u", "--user"},
            description = "Database username",
            required = true)
    String username;

    @CommandLine.Option(
            names = {"-pw", "--password"},
            description = "Database username",
            required = true)
    String password;

    @CommandLine.Option(
            names = {"-i", "--idle-seconds"},
            description = "Workload pause",
            defaultValue = "360")
    int inactivitySeconds;

    public TestAmsResumeCanaryV2() {
    }

    public static void main(String... args) throws Exception {
        new Driver();
        int exitCode = new CommandLine(new TestAmsResumeCanaryV2()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < clusters; i++) {
            Thread t = new Thread(new TestAmsResumeCanaryV2Worker(this));
            threads.add(t);
            t.start();
        }

        while (!Thread.interrupted()) {
            System.out.println("Started " + clusters + " canary workers");
            Threads.sleep(10000);
        }

        return 0;
//        int run = 0;
//        while (!Thread.interrupted()) {
//            try {
//                System.out.println("Run: " + (run++));
//                boolean ready = driveQueriesUntilSuccessful();
//                if (ready) {
//                    System.out.println("Success!");
//                    stayIdle(inactivitySeconds);
//                    ResumeStats stats = resume();
//                    reportMetrics(stats);
//                } else {
//                    System.out.println("Not ready in time. Starting over.");
//                }
//            } catch (Exception e) {
//                Exceptions.capture(e);
//                Threads.sleep(1000);
//            }
//        }
//        return 0;
    }

    private void stayIdle(int inactivitySeconds) {
        for (int i = 0; i < inactivitySeconds; i++) {
            Threads.sleep(1000);
            System.out.println("Chilling out for " + i + " seconds...");
        }
    }


}