package co.kuznetsov;

import org.apache.commons.lang.math.RandomUtils;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterResponse;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.ServerlessV2ScalingConfiguration;
import software.amazon.awssdk.services.rds.model.Tag;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TestAmsResumeCanaryV2Worker implements Runnable {
    private static final long MAX_RESUME_WAIT_MILLIS = 90000;

    private final TestAmsResumeCanaryV2 canary;
    private final int workerId;

    public TestAmsResumeCanaryV2Worker(TestAmsResumeCanaryV2 testAmsResumeCanaryV2, int workerId) {
        this.canary = testAmsResumeCanaryV2;
        this.workerId = workerId;
    }

    @Override
    public void run() {
        AtomicReference<DBCluster>   clusterRef = new AtomicReference<>();
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

        String instanceId = instanceRef.get().dbInstanceIdentifier();

        int run = 0;
        while (!Thread.interrupted()) {
            try {
                System.out.println("Run [" + instanceId + "]: " + (run++));
                boolean ready = driveQueriesUntilSuccessful(instanceRef, endpoint, port);
                if (ready) {
                    System.out.println("Success [" + instanceId + "]");
                    stayIdle(instanceId, canary.inactivitySeconds + rnd(120));
                    ResumeStats stats = resume(instanceRef, endpoint, port);
                    reportMetrics(instanceRef, stats);
                } else {
                    System.out.println("Not ready in time [" + instanceId + "]. Starting over.");
                }
            } catch (Exception e) {
                Exceptions.capture(e);
                Threads.sleep(1000);
            }
        }
    }

    private int rnd(int max) {
        return RandomUtils.nextInt(max);
    }

    private void stayIdle(String instanceId, int inactivitySeconds) {
        for (int i = 0; i < inactivitySeconds; i++) {
            Threads.sleep(1000);
            System.out.println("Instance [" + instanceId + "] is chilling out for " + i + " seconds...");
        }
    }

    private void provisionACluster(AtomicReference<DBCluster> clusterRef, AtomicReference<DBInstance> instanceRef) {
        try (RdsClient rds = RdsClient.builder().endpointOverride(new URI(canary.rdsEndpoint)).build()) {
            String suffix = String.format("%03d", workerId);
            String clusterIdentifier = "persist-ams-ap-canary-c-" + suffix;
            String instanceIdentifier = "persist-ams-ap-canary-i-" + suffix;

            DBCluster existingCluster = describeDbCluster(rds, clusterIdentifier);
            if (existingCluster != null) {
                clusterRef.set(existingCluster);
            } else {
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
                            .masterUsername(canary.username)
                            .masterUserPassword(canary.password)
                            .databaseName(canary.database)
                            .build();

                    CreateDbClusterResponse createdCluster = rds.createDBCluster(createDbClusterRequest);
                    clusterRef.set(createdCluster.dbCluster());
                });
            }

            DBInstance existingInstance = describeDbInstance(rds, instanceIdentifier);
            if (existingInstance != null) {
                instanceRef.set(existingInstance);
            } else {
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
            }

            System.out.println("Waiting for instance to be ready: " + instanceIdentifier);
            rds.waiter().waitUntilDBInstanceAvailable(DescribeDbInstancesRequest.builder()
                        .dbInstanceIdentifier(instanceIdentifier)
                        .build());
        } catch (URISyntaxException e) {
            Exceptions.capture(e);
            System.exit(-1);
        }
    }

    private DBInstance describeDbInstance(RdsClient rds, String instanceIdentifier) {
        AtomicReference<DBInstance> ref = new AtomicReference<>();
        Threads.retryUntilSuccess(() -> {
            try {
                var request = DescribeDbInstancesRequest.builder()
                        .dbInstanceIdentifier(instanceIdentifier)
                        .build();
                var response = rds.describeDBInstances(request);
                if (!response.dbInstances().isEmpty()) {
                    ref.set(response.dbInstances().get(0));
                }
            } catch (DbInstanceNotFoundException e) {
                ref.set(null);
            }
        });
        return ref.get();
    }

    private DBCluster describeDbCluster(RdsClient rds, String clusterIdentifier) {
        AtomicReference<DBCluster> ref = new AtomicReference<>();
        Threads.retryUntilSuccess(() -> {
            try {
                var request = DescribeDbClustersRequest.builder()
                        .dbClusterIdentifier(clusterIdentifier)
                        .build();
                var response = rds.describeDBClusters(request);
                if (!response.dbClusters().isEmpty()) {
                    ref.set(response.dbClusters().get(0));
                }
            } catch (DbClusterNotFoundException e) {
                ref.set(null);
            }
        });
        return ref.get();
    }

    private boolean driveQueriesUntilSuccessful(AtomicReference<DBInstance> instanceRef, String endpoint, int port) {
        System.out.println("Probing the endpoint [" + instanceRef.get().dbInstanceIdentifier() + "]");
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

    private ResumeStats resume(AtomicReference<DBInstance> instanceRef, String endpoint, int port) {
        System.out.println("Starting resume [" + instanceRef.get().dbInstanceIdentifier() + "]...");
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
            System.out.println("Failure to connect to " + endpoint + ":" + port);
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
                    .value((double) (outcome.isFailure() ? 0 : outcome.didSleep() ? 1 : 0))
                    .build();
            var noSleep = MetricDatum.builder()
                    .metricName("noSleep")
                    .unit(StandardUnit.COUNT)
                    .timestamp(now)
                    .value((double) (outcome.isFailure() ? 0 : outcome.didSleep() ? 0 : 1))
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
                    .value((double) (outcome.isFailure() ? 0 : outcome.didSleep() ? 1 : 0))
                    .build();
            var noSleepInstance = MetricDatum.builder()
                    .metricName("noSleep")
                    .unit(StandardUnit.COUNT)
                    .dimensions(instanceDimension)
                    .timestamp(now)
                    .value((double) (outcome.isFailure() ? 0 : outcome.didSleep() ? 0 : 1))
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
                            noSleep,
                            noSleepInstance,
                            failure,
                            failureInstance,
                            clientInterrupt,
                            clientInterruptInstance
                    ).namespace(canary.getMetricsNamespace())
                    .build();

            Threads.retryUntilSuccess(() -> {
                cw.putMetricData(dataRequest);
            });

            List<InputLogEvent> inputLogEvents = new ArrayList<>();
            inputLogEvents.add(InputLogEvent.builder()
                            .message(String.format(
                                    "Resume for instanceId: %s. Outcome: [success=%b, noSleep=%b, clientInterrupt=%b, connectionDrop=%b, duration=%d, durationHighRes=%d]",
                                    instanceRef.get().dbInstanceIdentifier(),
                                    !outcome.isFailure() && outcome.didSleep(),
                                    !outcome.isFailure() && !outcome.didSleep(),
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
                            .logGroupName(canary.getLogGroupName())
                            .logStreamName(canary.getLogStreamName())
                            .logEvents(inputLogEvents)
                            .build()

                );
            });

            System.out.println("Posted metrics to CW: " + outcome);
        }
    }

}
