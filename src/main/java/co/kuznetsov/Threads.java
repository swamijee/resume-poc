package co.kuznetsov;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class Threads {

    private static final long THREAD_WAIT_FOR_INTERRUPT_MILLIS = 5000;

    private Threads() {
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Exceptions.capture(e);
        }
    }

    public static void retryUntilSuccess(Runnable run) {
        boolean success = false;
        while (!success) {
            try {
                run.run();
                success = true;
            } catch (Exception t) {
                Exceptions.capture(t, "Failed to run the action");
                Threads.sleep(1000);
            }
        }
    }
}
