package co.kuznetsov;


import com.mysql.cj.jdbc.Driver;
import picocli.CommandLine;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

@CommandLine.Command(name = "test-eni-move", mixinStandardHelpOptions = true,
        description = "Test ENI moves")
public class TestAmsResumeCanary implements Callable<Integer> {
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
            names = {"-i", "--idle-seconds"},
            description = "Workload pause",
            defaultValue = "360")
    private int inactivitySeconds;

    public TestAmsResumeCanary() {
    }

    public static void main(String... args) throws Exception {
        new Driver();
        int exitCode = new CommandLine(new TestAmsResumeCanary()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        int run = 0;
        while (!Thread.interrupted()) {
            try {
                System.out.println("Run: " + (run++));
                boolean ready = driveQueriesUntilSuccessful();
                if (ready) {
                    stayIdle(inactivitySeconds);
                    ResumeStats stats = resume();
                    reportMetrics(stats);
                } else {
                    System.out.println("Not ready in time. Starting over.");
                }
            } catch (Exception e) {
                Exceptions.capture(e);
                Threads.sleep(1000);
            }
        }
        return 0;
    }

    private void stayIdle(int inactivitySeconds) {
        for (int i = 0; i < inactivitySeconds; i++) {
            Threads.sleep(1000);
            System.out.println("Chilling out for " + i + " seconds...");
        }
    }

    private boolean driveQueriesUntilSuccessful() {
        System.out.println("Probing the endpoint....");
        AtomicReference<ResumeOutcome> outcomeHfRef = new AtomicReference<>(null);

        Thread connectionThreadHF = new Thread(new HFDoorKnockRunnable(
                outcomeHfRef,
                endpoint,
                port,
                username,
                password,
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
                ).namespace("ASv2AMSAutoPauseCanary")
                .build();
        Threads.retryUntilSuccess(() -> {
            cw.putMetricData(dataRequest);
        });
        System.out.println("Posted metrics to CW: " + outcome);
    }
}