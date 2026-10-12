package didatrade.util;

import didatrade.DidaTradePaxos;

import java.util.*;

/**
 * Step 3 recovery scaffold, independent from Classic/Multi's PhaseOneResponseProcessor.
 * Keep complete promises per acceptor; do not collapse equal-ballot votes on arrival.
 * Implementation order: Docs/step3/Step-3-Implementation-Guide.md.
 */
public final class FastPhaseOneResponseProcessor
        extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {
    private final int                                        ballot;
    private final Set<Integer>                               acceptors;
    private final int                                        quorum;
    private final int                                        fastQuorum;
    private final Map<Integer, DidaTradePaxos.PhaseOneReply> promises  = new HashMap<>();
    private       int                                        maxballot = -1;
    private       boolean                                    accepted  = false;
    private       boolean                                    finished  = false;

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
                || fastQuorum < 1 || fastQuorum > n
                || 2L * fastQuorum <= n
                || recoveryQuorum + 2L * fastQuorum <= 2L * n) {
            throw new IllegalArgumentException("Invalid Fast recovery quorums/configuration");
        }
        this.quorum = recoveryQuorum;
        this.fastQuorum = fastQuorum;
    }

    @Override
    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> allResponses,
                                       DidaTradePaxos.PhaseOneReply response) {
        if (finished) return true;

        if (response.getRequestballot() != ballot || !acceptors.contains(response.getServerid())) return false;

        if (promises.containsKey(response.getServerid())) return false;

        maxballot = Math.max(maxballot, response.getMaxballot());

        if (!response.getAccepted()) {
            finished = true;
            return true;
        }

        promises.put(response.getServerid(), response);

        if (promises.size() >= quorum) {
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

    public synchronized Map<Integer, DidaTradePaxos.AcceptedEntry> getRecoveredEntries() {
        if (!accepted) {
            throw new IllegalStateException("Phase 1 has no successful quorum");
        }
        Map<Integer, List<DidaTradePaxos.AcceptedEntry>> entriesByInstance = new HashMap<>();

        for (var promise : promises.values()) {
            Set<Integer> seenInstances = new HashSet<>();

            for (var entry : promise.getAcceptedEntriesList()) {
                if (!seenInstances.add(entry.getInstance())) {
                    throw new IllegalStateException("Duplicate instance in acceptor promise");
                }
                int instance = entry.getInstance();

                List<DidaTradePaxos.AcceptedEntry> entries = entriesByInstance.get(instance);

                if (entries == null) {
                    entries = new ArrayList<>();
                    entriesByInstance.put(instance, entries);
                }

                entries.add(entry);
            }
        }

        int n = acceptors.size();
        // Minimum overlap of this recovery quorum with a previous Fast decision.
        int threshold = quorum + fastQuorum - n;

        Map<Integer, DidaTradePaxos.AcceptedEntry> recovered = new HashMap<>();

        for (var group : entriesByInstance.entrySet()) {

            int highestBallot = -1;

            for (DidaTradePaxos.AcceptedEntry entry : group.getValue()) {
                if (entry.getValballot() > highestBallot) {
                    highestBallot = entry.getValballot();
                }
            }

            Map<Integer, Integer>                      votes      = new HashMap<>();
            Map<Integer, DidaTradePaxos.AcceptedEntry> candidates = new TreeMap<>();

            for (var entry : group.getValue()) {
                if (entry.getValballot() != highestBallot) continue;

                votes.merge(entry.getValue(), 1, Integer::sum);
                candidates.put(entry.getValue(), entry);
            }

            int chosenValue = candidates.keySet().iterator().next();

            for (var vote : votes.entrySet()) {
                if (vote.getValue() >= threshold) {
                    chosenValue = vote.getKey();
                    break;
                }
            }

            recovered.put(group.getKey(), candidates.get(chosenValue));
        }

        return recovered;
    }
}
