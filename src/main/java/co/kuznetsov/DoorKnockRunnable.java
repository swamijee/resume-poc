package co.kuznetsov;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class DoorKnockRunnable implements Runnable {
    private final AtomicReference<ResumeOutcome> outcomeRef;
    private final String endpoint;
    private final int port;
    private final String username;
    private final String password;
    private final String dbName;
    private final long maxWaitMillis;
    private final String engineDriver;

    public DoorKnockRunnable(AtomicReference<ResumeOutcome> outcomeRef, String endpoint, int port, String username, String password, long maxWaitMillis) {
        this(outcomeRef, endpoint, port, "mysql", username, password, "", maxWaitMillis);
    }

    public DoorKnockRunnable(AtomicReference<ResumeOutcome> outcomeRef, String endpoint, int port, String engineDriver, String username, String password, String dbName, long maxWaitMillis) {
        this.outcomeRef = outcomeRef;
        this.endpoint = endpoint;
        this.port = port;
        this.engineDriver = engineDriver;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.maxWaitMillis = maxWaitMillis;
    }


    @Override
    public void run() {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("connectTimeout", "45000");

        long start = System.currentTimeMillis();
        boolean drop = false;

        while ((System.currentTimeMillis() - start) < maxWaitMillis) {
            System.out.println("Connecting to DB...");
            try (var conn = DriverManager.getConnection("jdbc:" + engineDriver + "://" + endpoint + ":" + port + "/" + dbName, properties)) {
                conn.createStatement().execute("SELECT 1");
                System.out.println("Success!");
            } catch (SQLException e) {
                Exceptions.capture(e);
                drop = true;
                continue;
            }
            var durationMillis = System.currentTimeMillis() - start;
            outcomeRef.set(new ResumeOutcome(drop, false, durationMillis, false));
            return;
        }
        var durationMillis = System.currentTimeMillis() - start;
        outcomeRef.set(new ResumeOutcome(true, true, durationMillis, false));
    }
}
