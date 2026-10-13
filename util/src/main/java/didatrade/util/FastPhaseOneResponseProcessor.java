package didatrade.util;

import didatrade.DidaTradePaxos.AcceptedEntry;
import didatrade.DidaTradePaxos.PhaseOneReply;

import java.util.*;

/** Keeps one complete promise per acceptor to recover each log position independently. */
public final class FastPhaseOneResponseProcessor
        extends GenericResponseProcessor<PhaseOneReply> {
    private final int ballot;
    private final Set<Integer> acceptors;
    private final int recoveryQuorum;
    private final int fastQuorum;
    private final Map<Integer, PhaseOneReply> promises = new HashMap<>();
    private int maxballot = -1;
    private boolean accepted;
    private boolean finished;

    public FastPhaseOneResponseProcessor(int ballot, List<Integer> acceptors, int quorum) {
        this(ballot, acceptors, quorum, quorum);
    }

    public FastPhaseOneResponseProcessor(int ballot, List<Integer> acceptors,
                                         int recoveryQuorum, int fastQuorum) {
        this.ballot = ballot;
        this.acceptors = Set.copyOf(acceptors);
        int n = this.acceptors.size();
        if (ballot < 0 || n == 0 || n != acceptors.size()
                || recoveryQuorum < 1 || recoveryQuorum > n
                || fastQuorum < 1 || fastQuorum > n) {
            throw new IllegalArgumentException("Invalid Fast recovery quorums/configuration");
        }
        // Recovery must intersect the overlap of any two Fast decision quorums.
        if (recoveryQuorum + 2L * fastQuorum <= 2L * n) {
            throw new IllegalArgumentException("Invalid Fast recovery quorums/configuration");
        }
        this.recoveryQuorum = recoveryQuorum;
        this.fastQuorum = fastQuorum;
    }

    @Override
    public synchronized boolean onNext(ArrayList<PhaseOneReply> allResponses,
                                       PhaseOneReply response) {
        if (finished) return true;

        if (response.getRequestballot() != ballot || !acceptors.contains(response.getServerid())) return false;

        if (promises.containsKey(response.getServerid())) return false;

        maxballot = Math.max(maxballot, response.getMaxballot());

        if (!response.getAccepted()) {
            finished = true;
            return true;
        }

        promises.put(response.getServerid(), response);

        if (promises.size() >= recoveryQuorum) {
            accepted = true;
            finished = true;
        }

        return finished;
    }


    public synchronized boolean getAccepted() {
        return accepted;
    }

    public synchronized int getMaxballot() {
        return maxballot;
    }

    public synchronized Map<Integer, AcceptedEntry> getRecoveredEntries() {
        if (!accepted) {
            throw new IllegalStateException("Phase 1 has no successful quorum");
        }
        Map<Integer, List<AcceptedEntry>> entriesByInstance = new HashMap<>();

        for (PhaseOneReply promise : promises.values()) {
            Set<Integer> seenInstances = new HashSet<>();

            for (AcceptedEntry entry : promise.getAcceptedEntriesList()) {
                if (!seenInstances.add(entry.getInstance())) {
                    throw new IllegalStateException("Duplicate instance in acceptor promise");
                }
                int instance = entry.getInstance();

                List<AcceptedEntry> entries = entriesByInstance.get(instance);

                if (entries == null) {
                    entries = new ArrayList<>();
                    entriesByInstance.put(instance, entries);
                }

                entries.add(entry);
            }
        }

        int n = acceptors.size();
        // Minimum overlap of this recovery quorum with a previous Fast decision.
        int threshold = recoveryQuorum + fastQuorum - n;

        Map<Integer, AcceptedEntry> recovered = new HashMap<>();

        for (Map.Entry<Integer, List<AcceptedEntry>> group : entriesByInstance.entrySet()) {

            int highestBallot = group.getValue().get(0).getValballot();

            for (AcceptedEntry entry : group.getValue()) {
                if (entry.getValballot() > highestBallot) {
                    highestBallot = entry.getValballot();
                }
            }

            Map<Integer, Integer> votes = new HashMap<>();
            AcceptedEntry chosen = null;
            for (AcceptedEntry entry : group.getValue()) {
                if (entry.getValballot() != highestBallot) continue;
                Integer count = votes.get(entry.getValue());
                if (count == null) count = 0;
                votes.put(entry.getValue(), count + 1);
                if (chosen == null || entry.getValue() < chosen.getValue()) chosen = entry;
            }

            // A possible earlier decision overrides the deterministic minimum-ID tie break.
            for (AcceptedEntry entry : group.getValue()) {
                if (entry.getValballot() == highestBallot && votes.get(entry.getValue()) >= threshold) {
                    chosen = entry;
                    break;
                }
            }
            recovered.put(group.getKey(), chosen);
        }

        return recovered;
    }
}
