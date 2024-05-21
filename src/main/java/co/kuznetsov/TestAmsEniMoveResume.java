package co.kuznetsov;


import com.google.common.collect.Iterables;
import com.mysql.cj.jdbc.Driver;
import picocli.CommandLine;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@CommandLine.Command(name = "test-eni-move", mixinStandardHelpOptions = true,
        description = "Test ENI moves")
public class TestAmsEniMoveResume implements Callable<Integer> {
    private static final long MAX_RESUME_WAIT_MILLIS = 90000;
    private static final long MAX_ENI_OP_WAIT_MILLIS = 120000;
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

    public static void main(String... args) throws Exception {
        new Driver();
        int exitCode = new CommandLine(new TestAmsEniMoveResume()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        int run = 0;
        while (!Thread.interrupted()) {
            try {
                System.out.println("Run: " + (run++));
                parkEniWithASleeper();
                ResumeStats stats = resume();
                reportMetrics(stats);
            } catch (Exception e) {
                Exceptions.capture(e);
                Threads.sleep(1000);
            }
        }
        return 0;
    }

    private ResumeStats resume() {
        System.out.println("Starting resume...");
        AtomicReference<ResumeOutcome> outcomeRef = new AtomicReference<>(null);
        AtomicReference<ResumeOutcome> outcomeHfRef = new AtomicReference<>(null);

        Thread connectionThread = new Thread(new DoorKnockRunnable(
                outcomeRef,
                endpoint,
                port,
                username,
                password,
                MAX_RESUME_WAIT_MILLIS
        ));
        Thread connectionThreadHF = new Thread(new HFDoorKnockRunnable(
                outcomeHfRef,
                endpoint,
                port,
                username,
                password,
                MAX_RESUME_WAIT_MILLIS
        ));

        connectionThread.start();
        connectionThreadHF.start();

        try {
            moveEniToDbInstance();
            connectionThread.join(MAX_RESUME_WAIT_MILLIS);
            connectionThreadHF.join(MAX_RESUME_WAIT_MILLIS);
            if (connectionThread.isAlive()) {
                connectionThread.interrupt();
                outcomeRef.set(new ResumeOutcome(true, true, MAX_RESUME_WAIT_MILLIS, false));
            }
            // By no outcomeRef is surely set
            return new ResumeStats(outcomeRef.get(), outcomeHfRef.get());
        } catch (InterruptedException e) {
            return new ResumeStats(new ResumeOutcome(false, false, -1, true), null);
        } catch (Exception e) {
            Exceptions.capture(e);
            return new ResumeStats(new ResumeOutcome(false, true, -1, true), null);
        }
    }

    private void moveEniToDbInstance() throws Exception {
        try (Ec2Client ec2 = Ec2Client.builder().build()) {
            // Making sure DB ENI is detached
            DescribeNetworkInterfacesRequest describeInterfaces = DescribeNetworkInterfacesRequest.builder().
                    networkInterfaceIds(dbEniId).build();

            DescribeNetworkInterfacesResponse netInterfacesDescription = ec2.describeNetworkInterfaces(describeInterfaces);
            NetworkInterface ni = Iterables.getOnlyElement(netInterfacesDescription.networkInterfaces());

            var attachment = ni.attachment();
            if (attachment != null) {
                System.out.println("Interface is attached, detaching...");
                String attachmentId = attachment.attachmentId();
                var detachRequest = DetachNetworkInterfaceRequest.builder().attachmentId(attachmentId).build();
                ec2.detachNetworkInterface(detachRequest);
            }

            waitWhileEni(dbEniId, eni -> (eni.status() != NetworkInterfaceStatus.AVAILABLE), "Waiting for DB ENI to detach from sleeper", MAX_ENI_OP_WAIT_MILLIS);

            // Attaching it to sleeper instance
            System.out.println("Attaching eni to DB instance");
            var attachRequest = AttachNetworkInterfaceRequest.builder()
                    .deviceIndex(1)
                    .networkInterfaceId(dbEniId)
                    .instanceId(dbInstanceId)
                    .build();
            System.out.println(ec2.attachNetworkInterface(attachRequest));

            System.out.println("Attach requested.");
            waitWhileEni(dbEniId, eni -> (eni.status() != NetworkInterfaceStatus.IN_USE), "Waiting for DB ENI to attach to DB", MAX_ENI_OP_WAIT_MILLIS);
        } catch (Exception e) {
            Exceptions.capture(e);
            throw e;
        }
    }

    private void reportMetrics(ResumeStats outcome) {
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
                .value((double) outcome.getResumeDuration())
                .timestamp(now)
                .unit(StandardUnit.MILLISECONDS)
                .build();
        var resumeDurationHighRes = MetricDatum.builder()
                .metricName("resumeDurationHighRes")
                .value((double) outcome.getResumeDurationHighRes())
                .timestamp(now)
                .unit(StandardUnit.MILLISECONDS)
                .build();
        var dataRequest = PutMetricDataRequest.builder()
                .metricData(
                        connectionDrop,
                        resumeDuration,
                        resumeDurationHighRes,
                        success,
                        failure,
                        clientInterrupt
                ).namespace("ASv2ResumeCanary")
                .build();
        Threads.retryUntilSuccess(() -> {
            cw.putMetricData(dataRequest);
        });
        System.out.println("Posted metrics to CW: " + outcome);
    }

    private void parkEniWithASleeper() throws Exception {
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
                Threads.retryUntilSuccess(() -> {
                    String attachmentId = attachment.attachmentId();
                    var detachRequest = DetachNetworkInterfaceRequest.builder().attachmentId(attachmentId).build();
                    ec2.detachNetworkInterface(detachRequest);
                });
            }

            waitWhileEni(dbEniId, eni -> (eni.status() != NetworkInterfaceStatus.AVAILABLE), "Waiting for detachment to be done", MAX_ENI_OP_WAIT_MILLIS);

            // Attaching it to sleeper instance
            Threads.retryUntilSuccess(() -> {
                var attachRequest = AttachNetworkInterfaceRequest.builder()
                        .deviceIndex(1)
                        .networkInterfaceId(dbEniId)
                        .instanceId(sleeperInstanceId)
                        .build();
                ec2.attachNetworkInterface(attachRequest);
            });

            waitWhileEni(dbEniId, eni -> (eni.status() != NetworkInterfaceStatus.IN_USE), "Waiting for ENI to attach to sleeper", MAX_ENI_OP_WAIT_MILLIS);
        } catch (Exception e) {
            Exceptions.capture(e);
            throw e;
        }
        System.out.println("Done parking!");
    }

    private void detachSleeperEni(Ec2Client ec2) throws Exception {
        DescribeNetworkInterfacesRequest describeSleeperEni = DescribeNetworkInterfacesRequest.builder().
                networkInterfaceIds(sleeperEni).build();
        DescribeNetworkInterfacesResponse netInterfacesDescription = ec2.describeNetworkInterfaces(describeSleeperEni);
        NetworkInterface ni = Iterables.getOnlyElement(netInterfacesDescription.networkInterfaces());
        var attachment = ni.attachment();

        if (attachment != null) {
            Threads.retryUntilSuccess(() -> {
                String attachmentId = attachment.attachmentId();
                var detachRequest = DetachNetworkInterfaceRequest.builder().attachmentId(attachmentId).build();
                ec2.detachNetworkInterface(detachRequest);
            });
        }

        waitWhileEni(sleeperEni, eni -> (eni.status() != NetworkInterfaceStatus.AVAILABLE), "Waiting for sleeper ENI to be detached", MAX_ENI_OP_WAIT_MILLIS);
    }

    private void waitWhileEni(String eniId, Function<NetworkInterface, Boolean> predicate, String message, long maxWait) throws TimeoutException {
        try {
            Ec2Client ec2 = Ec2Client.builder().build();
            DescribeNetworkInterfacesRequest describeInterfaces = DescribeNetworkInterfacesRequest.builder().
                    networkInterfaceIds(eniId).build();

            DescribeNetworkInterfacesResponse netInterfacesDescription = ec2.describeNetworkInterfaces(describeInterfaces);
            NetworkInterface ni = Iterables.getOnlyElement(netInterfacesDescription.networkInterfaces());

            if (!predicate.apply(ni)) {
                System.out.println("Wait not required. Exiting: " + ni.status());
                return;
            }

            long startedWaiting = System.currentTimeMillis();
            while (predicate.apply(ni)) {
                if ((System.currentTimeMillis() - startedWaiting) > maxWait) {
                    throw new TimeoutException("Operation [" + message + "] didn't complete for more than " + maxWait + "ms. Aborting wait.");
                }
                System.out.println(message + ": " + ni.status());
                try {
                    Thread.sleep(500);
                    DescribeNetworkInterfacesResponse updatedDescription = ec2.describeNetworkInterfaces(describeInterfaces);
                    ni = Iterables.getOnlyElement(updatedDescription.networkInterfaces());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            Exceptions.capture(e);
        }
    }

}