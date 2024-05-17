package co.kuznetsov;

public class ResumeOutcome {
    private final boolean connectionDrop;
    private final boolean failure;
    private final long duration;
    private final boolean clientInterrupt;

    public ResumeOutcome(boolean connectionDrop, boolean failure, long duration, boolean clientInterrupt) {
        this.connectionDrop = connectionDrop;
        this.failure = failure;
        this.duration = duration;
        this.clientInterrupt = clientInterrupt;
    }

    public boolean isClientInterrupt() {
        return clientInterrupt;
    }

    public boolean isConnectionDrop() {
        return connectionDrop;
    }

    public long getDuration() {
        return duration;
    }

    public boolean isFailure() {
        return failure;
    }
}
