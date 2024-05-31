package co.kuznetsov;


import com.mysql.cj.jdbc.Driver;
import picocli.CommandLine;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "test-ams-resumes", mixinStandardHelpOptions = true,
        description = "Test AMS resumes")
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
        ensureLogResourcesExist();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < clusters; i++) {
            Thread t = new Thread(new TestAmsResumeCanaryV2Worker(this, i));
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
            Threads.retryUntilSuccess(() -> {
                DescribeLogStreamsRequest describeLogStreams = DescribeLogStreamsRequest.builder()
                        .logGroupName("ASv2AMSAutoPauseCanary")
                        .logStreamNamePrefix("resume-outcomes.log")
                        .build();
                DescribeLogStreamsResponse response = cwl.describeLogStreams(describeLogStreams);
                if (response.logStreams().isEmpty()) {
                    CreateLogStreamRequest createLogStream = CreateLogStreamRequest.builder()
                            .logGroupName("ASv2AMSAutoPauseCanary")
                            .logStreamName("resume-outcomes.log")
                            .build();
                    cwl.createLogStream(createLogStream);
                    System.out.println("Created LogStream: \"resume-outcomes.log\"");
                }
            });
        }
    }

}