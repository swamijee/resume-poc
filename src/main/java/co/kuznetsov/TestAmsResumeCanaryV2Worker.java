package co.kuznetsov;

import org.apache.commons.lang.RandomStringUtils;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TestAmsResumeCanaryV2Worker implements Runnable {
    private static final long MAX_RESUME_WAIT_MILLIS = 90000;

    private final TestAmsResumeCanaryV2 canary;

    public TestAmsResumeCanaryV2Worker(TestAmsResumeCanaryV2 testAmsResumeCanaryV2) {
        this.canary = testAmsResumeCanaryV2;
    }

    @Override
    public void run() {
        AtomicReference<DBCluster> clusterRef = new AtomicReference<>();
        AtomicReference<DBInstance> instanceRef = new AtomicReference<>();

        while (!Thread.interrupted()) {
            if (clusterRef.get() == null && instanceRef.get() == null) {
                provisionACluster(clusterRef, instanceRef);
            }
            doAutoPausing(clusterRef, instanceRef);
        }
    }

    private void doAutoPausing(AtomicReference<DBCluster> clusterRef, AtomicReference<DBInstance> instanceRef) {
        String endpoint = clusterRef.get().endpoint();
        int port = clusterRef.get().port();

        int run = 0;
        while (!Thread.interrupted()) {
            try {
                System.out.println("Run [" + endpoint + "]: " + (run++));
                boolean ready = driveQueriesUntilSuccessful(endpoint, port);
                if (ready) {
                    System.out.println("Success [" + endpoint + "]");
                    stayIdle(canary.inactivitySeconds);
                    ResumeStats stats = resume(endpoint, port);
                    reportMetrics(instanceRef, stats);
                } else {
                    System.out.println("Not ready in time [" + endpoint + "]. Starting over.");
                }
            } catch (Exception e) {
                Exceptions.capture(e);
                Threads.sleep(1000);
            }
        }
    }

    private void stayIdle(int inactivitySeconds) {
        for (int i = 0; i < inactivitySeconds; i++) {
            Threads.sleep(1000);
            System.out.println("Chilling out for " + i + " seconds...");
        }
    }

    private void provisionACluster(AtomicReference<DBCluster> clusterRef, AtomicReference<DBInstance> instanceRef) {
        try (RdsClient rds = RdsClient.builder().endpointOverride(new URI(canary.rdsEndpoint)).build()) {
            String suffix = RandomStringUtils.randomAlphanumeric(6);
            String clusterIdentifier = "persist-ams-ap-canary-c-" + suffix;
            String instanceIdentifier = "persist-ams-ap-canary-i-" + suffix;

            Threads.retryUntilSuccess(() -> {
                CreateDbClusterRequest createDbClusterRequest = CreateDbClusterRequest.builder()
                        .tags(Tag.builder().key("asv2-ams-ap-canary").value("true").build())
                        .engine("aurora-mysql")
                        .dbClusterIdentifier(clusterIdentifier)
                        .engineVersion(canary.version)
                        .serverlessV2ScalingConfiguration(ServerlessV2ScalingConfiguration.builder()
                                .minCapacity(0.0)
                                .maxCapacity(8.0)
                                .build())
                        .vpcSecurityGroupIds(canary.securityGroup)
                        .databaseName(canary.database)
                        .build();

                CreateDbClusterResponse createdCluster = rds.createDBCluster(createDbClusterRequest);
                clusterRef.set(createdCluster.dbCluster());
            });

            Threads.retryUntilSuccess(() -> {
                CreateDbInstanceRequest createDbInstanceRequest = CreateDbInstanceRequest.builder()
                        .dbInstanceIdentifier(instanceIdentifier)
                        .dbClusterIdentifier(clusterIdentifier)
                        .dbInstanceClass("db.serverless")
                        .engine("aurora-mysql")
                        .build();

                CreateDbInstanceResponse createdInstance = rds.createDBInstance(createDbInstanceRequest);
                instanceRef.set(createdInstance.dbInstance());
            });

            rds.waiter().waitUntilDBInstanceAvailable(DescribeDbInstancesRequest.builder()
                    .dbInstanceIdentifier(instanceIdentifier)
                    .build());
        } catch (URISyntaxException e) {
            Exceptions.capture(e);
            System.exit(-1);
        }
    }

    private boolean driveQueriesUntilSuccessful(String endpoint, int port) {
        System.out.println("Probing the endpoint....");
        AtomicReference<ResumeOutcome> outcomeHfRef = new AtomicReference<>(null);

        Thread connectionThreadHF = new Thread(new HFDoorKnockRunnable(
                outcomeHfRef,
                endpoint,
                port,
                canary.username,
                canary.password,
                MAX_RESUME_WAIT_MILLIS
        ));

        connectionThreadHF.start();

        try {
            connectionThreadHF.join(MAX_RESUME_WAIT_MILLIS);
            if (connectionThreadHF.isAlive()) {
                connectionThreadHF.interrupt();
                return false;
            }
            // By no outcomeRef is surely set
            ResumeOutcome resumeOutcome = outcomeHfRef.get();
            return resumeOutcome != null && !resumeOutcome.isFailure();
        } catch (InterruptedException e) {
            Exceptions.capture(e);
            return false;
        }
    }

    private ResumeStats resume(String endpoint, int port) {
        System.out.println("Starting resume...");
        AtomicReference<ResumeOutcome> outcomeRef = new AtomicReference<>(null);
        AtomicReference<ResumeOutcome> outcomeHfRef = new AtomicReference<>(null);

        Thread connectionThread = new Thread(new DoorKnockRunnable(
                outcomeRef,
                endpoint,
                port,
                canary.username,
                canary.password,
                MAX_RESUME_WAIT_MILLIS
        ));
        Thread connectionThreadHF = new Thread(new HFDoorKnockRunnable(
                outcomeHfRef,
                endpoint,
                port,
                canary.username,
                canary.password,
                MAX_RESUME_WAIT_MILLIS
        ));

        connectionThread.start();
        connectionThreadHF.start();

        try {
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

    private void reportMetrics(AtomicReference<DBInstance> instanceRef, ResumeStats outcome) {
        try (CloudWatchClient cw = CloudWatchClient.builder().build();
             CloudWatchLogsClient cwl = CloudWatchLogsClient.builder().build(); ) {
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

            Dimension instanceDimension = Dimension.builder()
                    .name("instanceId")
                    .value(instanceRef.get().dbInstanceIdentifier())
                    .build();

            var clientInterruptInstance = MetricDatum.builder()
                    .metricName("clientInterrupt")
                    .unit(StandardUnit.COUNT)
                    .dimensions(instanceDimension)
                    .timestamp(now)
                    .value((double) (outcome.isClientInterrupt() ? 1 : 0))
                    .build();
            var successInstance = MetricDatum.builder()
                    .metricName("success")
                    .unit(StandardUnit.COUNT)
                    .dimensions(instanceDimension)
                    .timestamp(now)
                    .value((double) (outcome.isFailure() ? 0 : 1))
                    .build();
            var failureInstance = MetricDatum.builder()
                    .metricName("failure")
                    .unit(StandardUnit.COUNT)
                    .dimensions(instanceDimension)
                    .timestamp(now)
                    .value((double) (outcome.isFailure() ? 1 : 0))
                    .build();
            var connectionDropInstance = MetricDatum.builder()
                    .metricName("connectionDrop")
                    .unit(StandardUnit.COUNT)
                    .dimensions(instanceDimension)
                    .timestamp(now)
                    .value((double) (outcome.isConnectionDrop() ? 1 : 0))
                    .build();
            var resumeDurationInstance = MetricDatum.builder()
                    .metricName("resumeDuration")
                    .value((double) outcome.getResumeDuration())
                    .timestamp(now)
                    .unit(StandardUnit.MILLISECONDS)
                    .dimensions(instanceDimension)
                    .build();
            var resumeDurationHighResInstance = MetricDatum.builder()
                    .metricName("resumeDurationHighRes")
                    .value((double) outcome.getResumeDurationHighRes())
                    .timestamp(now)
                    .dimensions(instanceDimension)
                    .unit(StandardUnit.MILLISECONDS)
                    .build();

            var dataRequest = PutMetricDataRequest.builder()
                    .metricData(
                            connectionDrop,
                            connectionDropInstance,
                            resumeDuration,
                            resumeDurationInstance,
                            resumeDurationHighRes,
                            resumeDurationHighResInstance,
                            success,
                            successInstance,
                            failure,
                            failureInstance,
                            clientInterrupt,
                            clientInterruptInstance
                    ).namespace("ASv2AMSAutoPauseCanary")
                    .build();

            Threads.retryUntilSuccess(() -> {
                cw.putMetricData(dataRequest);
            });

            List<InputLogEvent> inputLogEvents = new ArrayList<>();
            inputLogEvents.add(InputLogEvent.builder()
                            .message(String.format(
                                    "Resume for instanceId: %s. Outcome: [success=%b, clientInterrupt=%b, connectionDrop=%b, duration=%d, durationHighRes=%d]",
                                    instanceRef.get().dbInstanceIdentifier(),
                                    !outcome.isFailure(),
                                    outcome.isClientInterrupt(),
                                    outcome.isConnectionDrop(),
                                    outcome.getResumeDuration(),
                                    outcome.getResumeDurationHighRes()
                             ))
                            .timestamp(now.toEpochMilli())
                    .build());

            Threads.retryUntilSuccess(() -> {
                cwl.putLogEvents(
                    PutLogEventsRequest.builder()
                            .logGroupName("ASv2AMSAutoPauseCanary")
                            .logGroupName("resume-outcomes.log")
                            .logEvents(inputLogEvents)
                            .build()

                );
            });

            System.out.println("Posted metrics to CW: " + outcome);
        }
    }
}
