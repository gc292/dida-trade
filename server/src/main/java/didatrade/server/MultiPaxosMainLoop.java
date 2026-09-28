package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoAttempt;

import java.util.*;

public class MultiPaxosMainLoop implements ConsensusLoop {
    private final DidaTradeServerState                       server_state;
    private       int                                        prepared_ballot   = -1;
    private       Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries  = new HashMap<>();
    private       Map<Integer, Integer>                      assignedRequests  = new HashMap<>();
    private       Map<Integer, PhaseTwoAttempt>              phaseTwoAttempts  = new HashMap<>();
    private       boolean                                    has_work;
    private       int                                        next_new_instance = 0;
    private       TreeSet<Integer>                           holes             = new TreeSet<>();


    public MultiPaxosMainLoop(DidaTradeServerState state) {
        server_state = state;
        has_work     = false;
    }

    @Override
    public void run() {
        while (true) {
            synchronized (this) {
                while (!has_work) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                    }
                }
                has_work = false;
            }

            int ballot = server_state.getCurrentBallot();

            if (!prepareLeadership(ballot)) {
                continue;
            }

            for (int instance : recoveredEntries.keySet()) {
                startProposal(instance, ballot);
            }

            while (true) {
                Integer position = assignNextPendingInstance();

                if (position == null) {
                    break;
                }

                startProposal(position, ballot);
            }
        }
    }

    @Override
    public synchronized void wakeup() {
        has_work = true;
        notify();
    }

    private boolean prepareLeadership(int ballot) {
        if (server_state.getCurrentBallot() != ballot || server_state.scheduler.leader(ballot) != server_state.my_id) {
            return false;
        }

        if (prepared_ballot == ballot) {
            return true;
        }

        PhaseOneResponseProcessor processor = server_state.runPhaseOne(0, ballot, server_state.getCompletedBallot());

        if (!processor.getAccepted()) {
            server_state.setCurrentBallot(processor.getMaxballot());
            return false;
        }

        if (server_state.getCurrentBallot() != ballot) {
            return false;
        }

        recoveredEntries = processor.getRecoveredEntries();

        assignedRequests.clear();
        holes.clear();
        next_new_instance = 0;

        for (DidaTradePaxos.AcceptedEntry entry : recoveredEntries.values()) {
            assignedRequests.put(entry.getInstance(), entry.getValue());

            next_new_instance = Math.max(next_new_instance, entry.getInstance() + 1);
        }

        for (int position = 0; position < next_new_instance; position++) {
            if (!recoveredEntries.containsKey(position)) {
                holes.add(position);
            }

        }
        prepared_ballot = ballot;
        return true;
    }

    private RequestRecord assignPendingRequest(int instance) {

        Integer idreservado = assignedRequests.get(instance);

        if (idreservado != null) {
            return server_state.req_history.getIfPending(idreservado);
        }

        Set<Integer>  excludedIds = new HashSet<>(assignedRequests.values());
        RequestRecord request     = server_state.req_history.getFirstPendingExcluding(excludedIds);

        if (request == null) {
            return null;
        }

        assignedRequests.put(instance, request.getId());
        return request;
    }

    // Returns the reserved log position, or null when no pending request is available.
    private Integer assignNextPendingInstance() {
        boolean fillingHole = !holes.isEmpty();
        int     instance    = fillingHole ? holes.first() : next_new_instance;

        RequestRecord request = assignPendingRequest(instance);
        if (request == null) {
            return null;
        }

        if (fillingHole) {
            holes.remove(instance);
        } else {
            next_new_instance++;
        }

        return instance;
    }

    private void startProposal(int instance, int ballot) {
        if (phaseTwoAttempts.containsKey(instance)) {
            return;
        }

        if (prepared_ballot != ballot
                || server_state.getCurrentBallot() != ballot
                || server_state.scheduler.leader(ballot) != server_state.my_id) {
            return;
        }

        Integer value = selectProposalValue(instance);
        if (value == null) {
            return;
        }

        PhaseTwoAttempt attempt = server_state.startPhaseTwo(instance, ballot, value);
        phaseTwoAttempts.put(instance, attempt);
        attempt.onCompletion(this::wakeup);
    }

    private Integer selectProposalValue(int instance) {
        DidaTradePaxos.AcceptedEntry recovered = recoveredEntries.get(instance);

        if (recovered != null) {
            return recovered.getValue();
        }

        RequestRecord request = assignPendingRequest(instance);

        if (request == null) {
            return null;
        }

        return request.getId();
    }


}
