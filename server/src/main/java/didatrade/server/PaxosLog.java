package didatrade.server;

import didatrade.DidaTradePaxos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;


public class PaxosLog {
    private Hashtable<Integer, PaxosInstance> log;
    private Map<Integer, Map<Integer, LearnedValue>> learnedValues;

    private static final class LearnedValue {
        private final int          value;
        private final Set<Integer> acceptors = new HashSet<>();

        private LearnedValue(int value) {
            this.value = value;
        }
    }

    public PaxosLog() {
        this.log           = new Hashtable<Integer, PaxosInstance>();
        this.learnedValues = new HashMap<>();
    }

    public synchronized PaxosInstance testAndSetEntry(int position) {
        PaxosInstance entry = this.log.get(position);

        if (entry == null) {
            entry = new PaxosInstance(position);
            this.log.put(position, entry);
        }
        return entry;
    }

    public synchronized boolean isDecided(int position) {
        PaxosInstance entry = this.log.get(position);
        return entry != null && entry.decided;
    }

    public synchronized <T> T startIfUndecided(int position, Supplier<T> starter) {
        if (isDecided(position)) return null;
        return starter.get();
    }

    public synchronized boolean accept(int position, int ballot, int commandId) {
        PaxosInstance entry = testAndSetEntry(position);

        if (entry.decided && entry.command_id != commandId) return false;
        if (ballot < entry.write_ballot) return false;
        if (ballot == entry.write_ballot && entry.accepted_command_id != commandId) return false;

        entry.accepted_command_id = commandId;
        entry.write_ballot = ballot;
        return true;
    }

    public synchronized ArrayList<DidaTradePaxos.AcceptedEntry> getAcceptedEntries() {
        ArrayList<DidaTradePaxos.AcceptedEntry> entries = new ArrayList<>();

        for (PaxosInstance entry : this.log.values()) {
            if (entry.write_ballot < 0) continue;

            entries.add(DidaTradePaxos.AcceptedEntry.newBuilder()
                    .setInstance(entry.instance_nb)
                    .setValue(entry.accepted_command_id)
                    .setValballot(entry.write_ballot)
                    .build());
        }
        return entries;
    }

    public synchronized Map<Integer, Integer> getDecidedEntriesFrom(int firstPosition) {
        Map<Integer, Integer> entries = new HashMap<>();

        for (PaxosInstance entry : this.log.values()) {
            if (entry.decided && entry.instance_nb >= firstPosition) {
                entries.put(entry.instance_nb, entry.command_id);
            }
        }
        return entries;
    }

    public synchronized Integer getDecidedCommandId(int position) {
        PaxosInstance entry = this.log.get(position);
        if (entry == null || !entry.decided) return null;
        return entry.command_id;
    }

    public synchronized void decideEntry(int position, int ballot, int commandId) {
        PaxosInstance entry = this.log.get(position);

        if (entry == null) {
            entry = new PaxosInstance(position);
            this.log.put(position, entry);
        }

        if (entry.decided && entry.command_id != commandId) {
            throw new IllegalStateException("Paxos instance already decided with a different value.");
        }

        entry.command_id      = commandId;
        entry.decision_ballot = Math.max(entry.decision_ballot, ballot);
        entry.decided         = true;
    }

    public synchronized boolean recordLearn(
            int position, int ballot, int commandId, int acceptor, int quorum) {
        PaxosInstance entry = testAndSetEntry(position);

        // An isolated losing vote from an earlier round is not a conflicting decision.
        if (entry.decided) return false;

        Map<Integer, LearnedValue> byBallot = learnedValues.computeIfAbsent(position, ignored -> new HashMap<>());
        LearnedValue               learned  = byBallot.computeIfAbsent(ballot, ignored -> new LearnedValue(commandId));

        if (learned.value != commandId) {
            throw new IllegalStateException("Different values learned in the same ballot and instance.");
        }

        learned.acceptors.add(acceptor);
        if (learned.acceptors.size() < quorum) return false;

        decideEntry(position, ballot, commandId);
        learnedValues.remove(position);
        return true;
    }

    public synchronized int findMaxDecidedBallot() {
        int ballot = -1;

        for (int position = 0; ; position++) {
            PaxosInstance entry = this.log.get(position);

            if (entry == null || !entry.decided) return ballot;
            ballot = Math.max(ballot, entry.decision_ballot);
        }
    }

}
