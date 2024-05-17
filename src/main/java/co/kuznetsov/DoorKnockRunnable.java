package co.kuznetsov;

import java.sql.Connection;
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
    private final long maxWaitMillis;

    public DoorKnockRunnable(AtomicReference<ResumeOutcome> outcomeRef, String endpoint, int port, String username, String password, long maxWaitMillis) {
        this.outcomeRef = outcomeRef;
        this.endpoint = endpoint;
        this.port = port;
        this.username = username;
        this.password = password;
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
            try (var conn = DriverManager.getConnection("jdbc://mysql://" + endpoint + ":" + port, properties)) {
                conn.createStatement().execute("SELECT 1");
            } catch (SQLException e) {
                System.out.println("SQLException: " + e.getMessage());
                System.out.println("SQLState: " + e.getSQLState());
                System.out.println("VendorError: " + e.getErrorCode());
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
