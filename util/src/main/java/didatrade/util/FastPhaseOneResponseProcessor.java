package didatrade.util;

import didatrade.DidaTradePaxos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Step 3 recovery scaffold, independent from Classic/Multi's PhaseOneResponseProcessor.
 * Keep complete promises per acceptor; do not collapse equal-ballot votes on arrival.
 * Implementation order: Design-decisions/Step-3-Implementation-Guide.md.
 */
public final class FastPhaseOneResponseProcessor
        extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {
    private final int ballot;
    private final Set<Integer> acceptors;
    private final int quorum;

    public FastPhaseOneResponseProcessor(int ballot, List<Integer> acceptors, int quorum) {
        this.ballot = ballot;
        this.acceptors = Set.copyOf(acceptors);
        this.quorum = quorum;
    }

    @Override
    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> allResponses,
                                       DidaTradePaxos.PhaseOneReply response) {
        // TODO FAST-03: validate identity/ballot, retain complete distinct promises, freeze Q.
        throw new UnsupportedOperationException("TODO FAST-03: collect Fast promises");
    }

    public synchronized boolean getAccepted() {
        // TODO FAST-03: successful promise quorum, not simply RPC completion.
        throw new UnsupportedOperationException("TODO FAST-03: Fast promise quorum result");
    }

    public synchronized int getMaxballot() {
        // TODO FAST-03: highest promise from valid replies, including rejections.
        throw new UnsupportedOperationException("TODO FAST-03: highest observed promise");
    }

    public synchronized Map<Integer, DidaTradePaxos.AcceptedEntry> getRecoveredEntries() {
        // TODO FAST-03: choose per position from the maximum accepted ballot in frozen Q.
        // Absent positions have no recovered concrete value; ANY is not an AcceptedEntry.
        throw new UnsupportedOperationException("TODO FAST-03: select Fast recovery values");
    }
}
