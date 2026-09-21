package didatrade.util;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

import java.util.*;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {
    private       ConfigurationScheduler                     scheduler;
    private       boolean                                    accepted;
    private       int                                        value;
    private       int                                        valballot;
    private       int                                        maxballot;
    private       int                                        low_ballot;
    private       int                                        high_ballot;
    private       Set<Integer>                               acceptedReplicas = new HashSet<>();
    private final Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries = new HashMap<>();


    public PhaseOneResponseProcessor(ConfigurationScheduler s, int l, int h) {
        this.accepted    = false;
        this.value       = -1;
        this.valballot   = -1;
        this.maxballot   = -1;
        this.low_ballot  = l;
        this.high_ballot = h;
        this.scheduler   = s;
    }

    public boolean getAccepted() {
        return this.accepted;
    }

    public int getValue() {
        return this.value;
    }

    public int getValballot() {
        return this.valballot;
    }

    public int getMaxballot() {
        return this.maxballot;
    }

    public synchronized Map<Integer, DidaTradePaxos.AcceptedEntry> getRecoveredEntries() {
        return new HashMap<>(this.recoveredEntries);
    }

    @Override
    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response) {
        this.maxballot = Math.max(this.maxballot, last_response.getMaxballot());

        // A rejection ends this attempt without a successful quorum.
        if (!last_response.getAccepted()) {
            this.accepted = false;
            return true;
        }

        this.acceptedReplicas.add(last_response.getServerid());

        for (DidaTradePaxos.AcceptedEntry entry : last_response.getAcceptedEntriesList()) {
            DidaTradePaxos.AcceptedEntry previous = recoveredEntries.get(entry.getInstance());

            if (previous == null || entry.getValballot() > previous.getValballot()) {
                recoveredEntries.put(entry.getInstance(), entry);
            }
        }

        // Preserve the value accepted in the highest previous ballot.
        if (last_response.getValballot() > this.valballot) {
            this.value     = last_response.getValue();
            this.valballot = last_response.getValballot();
        }

        if (this.acceptedReplicas.size() >= this.scheduler.quorum(this.high_ballot)) {
            this.accepted = true;
            return true;
        }

        return false;
    }
}
