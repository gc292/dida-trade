package didatrade.util;

import didatrade.DidaTradePaxos;

/** Tracks one Phase 2 attempt without requiring the caller to wait. */
public final class PhaseTwoAttempt {
    private final GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector;
    private final PhaseTwoResponseProcessor processor;

    public PhaseTwoAttempt(
            GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector,
            PhaseTwoResponseProcessor processor) {
        this.collector = collector;
        this.processor = processor;
    }

    /** Completion does not imply that a quorum accepted the proposal. */
    public boolean isDone() {
        return collector.isDone();
    }

    /** Registers one completion notification; completion does not imply acceptance.
     * Runs immediately if already complete. A second registration is rejected.
     */
    public void onCompletion(Runnable callback) {
        collector.onCompletion(callback);
    }

    public PhaseTwoResponseProcessor getResult() {
        // The collector monitor also makes response processing visible to this thread.
        if (!collector.isDone()) {
            throw new IllegalStateException("Phase 2 has not completed yet.");
        }
        return processor;
    }

    public PhaseTwoResponseProcessor waitUntilDone() {
        collector.waitUntilDone();
        return getResult();
    }
}
