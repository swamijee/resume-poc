package co.kuznetsov;


import com.google.common.collect.Iterables;
import picocli.CommandLine;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@CommandLine.Command(name = "test-eni-move", mixinStandardHelpOptions = true,
        description = "Test ENI moves")
public class TestAmsEniMoveResume implements Callable<Integer> {
    private static final long MAX_RESUME_WAIT_MILLIS = 90000;
    @CommandLine.Option(
            names = {"-e", "--endpoint"},
            description = "Endpoint",
            required = true)
    private String endpoint;

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "Endpoint",
            defaultValue = "3306")
    private int port;

    @CommandLine.Option(
            names = {"-u", "--user"},
            description = "Database username",
            required = true)
    private String username;

    @CommandLine.Option(
            names = {"-pw", "--password"},
            description = "Database username",
            required = true)
    private String password;

    @CommandLine.Option(
            names = {"-s", "--sleep-instance-id"},
            description = "Sleeper Instance ID",
            required = true)
    private String sleeperInstanceId;

    @CommandLine.Option(
            names = {"-d", "--db-instance-id"},
            description = "DB Instance ID",
            required = true)
    private String dbInstanceId;

    @CommandLine.Option(
            names = {"-db-eni", "--db-eni-id"},
            description = "DB ENI ID",
            required = true)
    private String dbEniId;

    @CommandLine.Option(
            names = {"-sleeper-eni", "--sleeper-eni-id"},
            description = "Sleeper ENI ID",
            required = true)
    private String sleeperEni;

    public TestAmsEniMoveResume() {
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new TestAmsEniMoveResume()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

        try {
            int run = 0;
            while (!Thread.interrupted()) {
                System.out.println("Run: " + (run++));
                parkEniWithASleeper();
                ResumeOutcome outcome = resume();
                reportMetrics(outcome);
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    private ResumeOutcome resume() {
        System.out.println("Starting resume...");
        AtomicReference<ResumeOutcome> outcomeRef = new AtomicReference<>(null);
        Thread connectionThread = new Thread(new DoorKnockRunnable(
                outcomeRef,
                endpoint,
                port,
                username,
                password,
                MAX_RESUME_WAIT_MILLIS
        ));
        connectionThread.start();

        try {
            moveEniToDbInstance();
            connectionThread.join(MAX_RESUME_WAIT_MILLIS);
            if (connectionThread.isAlive()) {
                connectionThread.interrupt();
                outcomeRef.set(new ResumeOutcome(true, true, MAX_RESUME_WAIT_MILLIS, false));
            }
            // By no outcomeRef is surely set
        } catch (InterruptedException e) {
            return new ResumeOutcome(false, false, -1, true);
        }
        return outcomeRef.get();
    }

    private void moveEniToDbInstance() {
        try (Ec2Client ec2 = Ec2Client.builder().build()) {
            // Making sure DB ENI is detached
            DescribeNetworkInterfacesRequest describeInterfaces = DescribeNetworkInterfacesRequest.builder().
                    networkInterfaceIds(dbEniId).build();

            DescribeNetworkInterfacesResponse netInterfacesDescription = ec2.describeNetworkInterfaces(describeInterfaces);
            NetworkInterface ni = Iterables.getOnlyElement(netInterfacesDescription.networkInterfaces());

            var attachment = ni.attachment();
            if (attachment != null) {
                String attachmentId = attachment.attachmentId();
                var detachRequest = DetachNetworkInterfaceRequest.builder().attachmentId(attachmentId).build();
                ec2.detachNetworkInterface(detachRequest);
            }

            waitWhileEni(dbEniId, eni -> (eni.attachment() != null), "Waiting for ENI to detach from sleeper");

            // Attaching it to sleeper instance
            var attachRequest = AttachNetworkInterfaceRequest.builder()
                    .deviceIndex(1)
                    .networkInterfaceId(dbEniId)
                    .instanceId(dbInstanceId)
                    .build();
            ec2.attachNetworkInterface(attachRequest);
            waitWhileEni(dbEniId, eni -> (eni.attachment() == null), "Waiting for ENI to attach to DB");
        }
    }

    private void reportMetrics(ResumeOutcome outcome) {
        CloudWatchClient cw = CloudWatchClient.builder().build();
        var now = Instant.now();

        var clientInterrupt = MetricDatum.builder()
                .metricName("clientInterrupt")
                .unit(StandardUnit.COUNT)
                .timestamp(now)
                .value((double) (outcome.isClientInterrupt() ? 1 : 0))
                .build();
        var success = MetricDatum.builder()
                .metricName("success")
                .unit(StandardUnit.COUNT)
                .timestamp(now)
                .value((double) (outcome.isFailure() ? 0 : 1))
                .build();
        var failure = MetricDatum.builder()
                .metricName("failure")
                .unit(StandardUnit.COUNT)
                .timestamp(now)
                .value((double) (outcome.isFailure() ? 1 : 0))
                .build();
        var connectionDrop = MetricDatum.builder()
                .metricName("connectionDrop")
                .unit(StandardUnit.COUNT)
                .timestamp(now)
                .value((double) (outcome.isConnectionDrop() ? 1 : 0))
                .build();
        var resumeDuration = MetricDatum.builder()
                .metricName("resumeDuration")
                .value((double) outcome.getDuration())
                .timestamp(now)
                .unit(StandardUnit.MILLISECONDS)
                .build();
        var dataRequest = PutMetricDataRequest.builder()
                .metricData(connectionDrop)
                .metricData(resumeDuration)
                .metricData(success)
                .metricData(failure)
                .metricData(clientInterrupt)
                .namespace("ASv2ResumeCanary")
                .build();
        cw.putMetricData(dataRequest);
        System.out.println("Posted metrics to CW!");
    }

    private void parkEniWithASleeper() {
        System.out.println("Parking ENI with a sleeper instance... ");
        try (Ec2Client ec2 = Ec2Client.builder().build()) {
            detachSleeperEni(ec2);

            // Making sure DB ENI is detached
            DescribeNetworkInterfacesRequest describeInterfaces = DescribeNetworkInterfacesRequest.builder().
                    networkInterfaceIds(dbEniId).build();

            DescribeNetworkInterfacesResponse netInterfacesDescription = ec2.describeNetworkInterfaces(describeInterfaces);
            NetworkInterface ni = Iterables.getOnlyElement(netInterfacesDescription.networkInterfaces());

            var attachment = ni.attachment();
            if (attachment != null) {
                String attachmentId = attachment.attachmentId();
                var detachRequest = DetachNetworkInterfaceRequest.builder().attachmentId(attachmentId).build();
                ec2.detachNetworkInterface(detachRequest);
            }

            waitWhileEni(dbEniId, eni -> (eni.attachment() != null), "Waiting for detachment to be done");

            // Attaching it to sleeper instance
            var attachRequest = AttachNetworkInterfaceRequest.builder()
                            .deviceIndex(1)
                            .networkInterfaceId(dbEniId)
                            .instanceId(sleeperInstanceId)
                            .build();
            ec2.attachNetworkInterface(attachRequest);

            waitWhileEni(dbEniId, eni -> (eni.attachment() == null), "Waiting for ENI to attach to sleeper");
        }
        System.out.println("Done parking!");
    }

    private void detachSleeperEni(Ec2Client ec2) {
        DescribeNetworkInterfacesRequest describeSleeperEni = DescribeNetworkInterfacesRequest.builder().
                networkInterfaceIds(sleeperEni).build();
        DescribeNetworkInterfacesResponse netInterfacesDescription = ec2.describeNetworkInterfaces(describeSleeperEni);
        NetworkInterface ni = Iterables.getOnlyElement(netInterfacesDescription.networkInterfaces());
        var attachment = ni.attachment();

        if (attachment != null) {
            String attachmentId = attachment.attachmentId();
            var detachRequest = DetachNetworkInterfaceRequest.builder().attachmentId(attachmentId).build();
            ec2.detachNetworkInterface(detachRequest);
        }

        waitWhileEni(dbEniId, eni -> (eni.attachment() != null), "Waiting for sleeper ENI to be detached");
    }

    private void waitWhileEni(String eniId, Function<NetworkInterface, Boolean> predicate, String message) {
        Ec2Client ec2 = Ec2Client.builder().build();
        DescribeNetworkInterfacesRequest describeInterfaces = DescribeNetworkInterfacesRequest.builder().
                networkInterfaceIds(eniId).build();

        DescribeNetworkInterfacesResponse netInterfacesDescription = ec2.describeNetworkInterfaces(describeInterfaces);
        NetworkInterface ni = Iterables.getOnlyElement(netInterfacesDescription.networkInterfaces());
        while (predicate.apply(ni)) {
            System.out.println(message);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}