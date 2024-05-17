package co.kuznetsov;

import co.kuznetsov.arb.account.Account;
import co.kuznetsov.arb.apps.activity.TradeExecutor;
import co.kuznetsov.arb.apps.activity.TestOrderListener;
import co.kuznetsov.arb.apps.base.ApplicationBase;
import co.kuznetsov.arb.broker.Broker;
import co.kuznetsov.arb.broker.ContractScreener;
import co.kuznetsov.arb.broker.ContractsStore;
import co.kuznetsov.arb.broker.trackers.AccountSummaryTracker;
import co.kuznetsov.arb.broker.trackers.FeedHealth;
import co.kuznetsov.arb.broker.trackers.FeedStatus;
import co.kuznetsov.arb.broker.trackers.PnLTracker;
import co.kuznetsov.arb.broker.types.MoneyValue;
import co.kuznetsov.arb.core.DDate;
import co.kuznetsov.arb.core.DurationCode;
import co.kuznetsov.arb.core.Market;
import co.kuznetsov.arb.core.Security;
import co.kuznetsov.arb.core.TTag;
import co.kuznetsov.arb.core.errors.SecurityNotFoundException;
import co.kuznetsov.arb.core.quality.SecurityUpdateLatencyStats;
import co.kuznetsov.arb.facet.signal.Signal;
import co.kuznetsov.arb.securities.Watchlist;
import co.kuznetsov.arb.strategy.Strategies;
import co.kuznetsov.arb.strategy.Strategy;
import co.kuznetsov.arb.strategy.StrategyTier;
import co.kuznetsov.arb.util.ColorConsole;
import co.kuznetsov.arb.util.Exceptions;
import co.kuznetsov.arb.util.Threads;
import picocli.CommandLine;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeNetworkInterfacesRequest;

import java.awt.Toolkit;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "test-eni-move", mixinStandardHelpOptions = true,
        description = "Test ENI moves")
public class TestEniMoveResume implements Callable<Integer> {

    @CommandLine.Option(
            names = {"-n", "--number-of-runs"},
            description = "Number of runs",
            defaultValue =  "1")
    private int runs;

    @CommandLine.Option(
            names = {"-e", "--endpoint"},
            description = "Endpoint",
            required = true)
    private String endpoint;

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

    public TestEniMoveResume() {
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new TestEniMoveResume()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        parkEniWithASleeper();

        return 0;
    }

    private void parkEniWithASleeper() {
        Ec2Client ec2 = Ec2Client.builder().build();
        DescribeNetworkInterfacesRequest describeInterfaces = DescribeNetworkInterfacesRequest.builder().networkInterfaceIds(dbEniId).build();
        ec2.describeNetworkInterfaces(describeInterfaces);
    }

}