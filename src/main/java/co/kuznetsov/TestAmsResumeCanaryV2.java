package co.kuznetsov;


import com.mysql.cj.jdbc.Driver;
import picocli.CommandLine;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
        ensureLogGroupExists();

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < clusters; i++) {
            Thread t = new Thread(new TestAmsResumeCanaryV2Worker(this));
            threads.add(t);
            t.start();
        }

        Instant started = Instant.now();
        while (!Thread.interrupted()) {
            System.out.println("Running " + clusters + " canary workers for " +
                    Duration.between(started, Instant.now()).truncatedTo(ChronoUnit.SECONDS));
            Threads.sleep(10000);
        }

        return 0;
    }

    private void ensureLogGroupExists() {
        try (CloudWatchLogsClient cwl = CloudWatchLogsClient.builder().build(); ) {
            Threads.retryUntilSuccess(() -> {
                DescribeLogGroupsRequest describeLogGroup = DescribeLogGroupsRequest.builder()
                        .logGroupNamePattern("ASv2AMSAutoPauseCanary")
                        .build();
                DescribeLogGroupsResponse response = cwl.describeLogGroups(describeLogGroup);
                if (response.logGroups().isEmpty()) {
                    CreateLogGroupRequest createLogGroup = CreateLogGroupRequest.builder()
                            .logGroupName("ASv2AMSAutoPauseCanary")
                            .build();
                    cwl.createLogGroup(createLogGroup);
                    System.out.println("Created LogGroup: \"ASv2AMSAutoPauseCanary\"");
                }
            });

        }
    }

}