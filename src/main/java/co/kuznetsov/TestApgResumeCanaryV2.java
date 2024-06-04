package co.kuznetsov;


import com.mysql.cj.jdbc.Driver;
import picocli.CommandLine;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "test-apg-resumes", mixinStandardHelpOptions = true,
        description = "Test APG resumes")
public class TestApgResumeCanaryV2 implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-v", "--version"},
            description = "Engine version",
            defaultValue = "15.7")
    public String version;

    @CommandLine.Option(
            names = {"-e", "--env-name"},
            description = "Environment name (e.g. mammoth, qa, us-east-1...)",
            required = true)
    public String envName;

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

    public TestApgResumeCanaryV2() {
    }

    public static void main(String... args) throws Exception {
        new Driver();
        int exitCode = new CommandLine(new TestApgResumeCanaryV2()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        ensureLogResourcesExist();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < clusters; i++) {
            Thread t = new Thread(new TestApgResumeCanaryV2Worker(this, i));
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

    private void ensureLogResourcesExist() {
        try (CloudWatchLogsClient cwl = CloudWatchLogsClient.builder().build(); ) {
            Threads.retryUntilSuccess(() -> {
                DescribeLogGroupsRequest describeLogGroup = DescribeLogGroupsRequest.builder()
                        .logGroupNamePattern(getLogGroupName())
                        .build();
                DescribeLogGroupsResponse response = cwl.describeLogGroups(describeLogGroup);
                if (response.logGroups().isEmpty()) {
                    CreateLogGroupRequest createLogGroup = CreateLogGroupRequest.builder()
                            .logGroupName(getLogGroupName())
                            .build();
                    cwl.createLogGroup(createLogGroup);
                    System.out.println("Created LogGroup: " + getLogGroupName());
                }
            });
            Threads.retryUntilSuccess(() -> {
                DescribeLogStreamsRequest describeLogStreams = DescribeLogStreamsRequest.builder()
                        .logGroupName(getLogGroupName())
                        .logStreamNamePrefix(getLogStreamName())
                        .build();
                DescribeLogStreamsResponse response = cwl.describeLogStreams(describeLogStreams);
                if (response.logStreams().isEmpty()) {
                    CreateLogStreamRequest createLogStream = CreateLogStreamRequest.builder()
                            .logGroupName(getLogGroupName())
                            .logStreamName(getLogStreamName())
                            .build();
                    cwl.createLogStream(createLogStream);
                    System.out.println("Created LogStream: " + getLogStreamName());
                }
            });
        }
    }

    protected String getLogGroupName() {
        return "ASv2APGAutoPauseCanary-" + envName;
    }

    public String getMetricsNamespace() {
        return "ASv2APGAutoPauseCanary-" + envName;
    }

    public String getLogStreamName() {
        return "resume-outcomes.log";
    }
}