package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.PhaseOneResponseProcessor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MultiPaxosMainLoop implements ConsensusLoop {
    private final DidaTradeServerState                       server_state;
    private       int                                        prepared_ballot  = -1;
    private       Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries = new HashMap<>();
    private       Map<Integer, Integer>                      assignedRequests = new HashMap<>();
    private       boolean                                    has_work;


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

            // Future proposal logic
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
        prepared_ballot  = ballot;
        return true;
    }

    private RequestRecord assignPendingRequest(int instance) {
        Set<Integer>  excludedIds = new HashSet<>(assignedRequests.values());
        RequestRecord request     = server_state.req_history.getFirstPendingExcluding(excludedIds);

        if (request == null) {
            return null;
        }

        assignedRequests.put(instance, request.getId());
        return request;
    }

}
