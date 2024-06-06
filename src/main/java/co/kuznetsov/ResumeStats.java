package co.kuznetsov;

public class ResumeStats {
    private final ResumeOutcome normal;
    private final ResumeOutcome highRes;
    public ResumeStats(ResumeOutcome longTimeout, ResumeOutcome highRes) {
        this.normal = longTimeout;
        this.highRes = highRes;
    }

    public boolean isClientInterrupt() {
        return normal.isClientInterrupt() || (highRes != null && highRes.isClientInterrupt());
    }


    public boolean isFailure() {
        return normal.isFailure() || (highRes != null && highRes.isFailure());
    }

    public boolean didSleep() {
        return normal.getDuration() > 250;
    }

    public boolean isConnectionDrop() {
        return normal.isConnectionDrop();
    }

    public Long getResumeDuration() {
        return normal.getDuration();
    }

    public Long getResumeDurationHighRes() {
        return highRes != null ? highRes.getDuration() : null;
    }

    @Override
    public String toString() {
        return "ResumeStats{" +
                "normal=" + normal +
                ", highRes=" + highRes +
                '}';
    }
}
