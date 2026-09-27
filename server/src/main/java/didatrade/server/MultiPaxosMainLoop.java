package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.PhaseOneResponseProcessor;

import java.util.HashMap;
import java.util.Map;

public class MultiPaxosMainLoop implements ConsensusLoop {
    private final DidaTradeServerState                       server_state;
    private       int                                        prepared_ballot  = -1;
    private       boolean                                    has_work;
    private       Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries = new HashMap<>();


    public MultiPaxosMainLoop(DidaTradeServerState state) {
        server_state = state;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            int ballot = server_state.getCurrentBallot();
            if (server_state.scheduler.leader(ballot) == server_state.my_id && prepared_ballot != ballot) {
                boolean prepared = prepareLeadership(ballot);
                if (!prepared && server_state.getCurrentBallot() != ballot) {
                    continue;
                }
            }
            synchronized (this) {
                while (!has_work && !Thread.currentThread().isInterrupted()){
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                has_work = false;
            } 

        }
    }

    @Override
    public synchronized void wakeup() {
        has_work = true;
        notifyAll();
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

}
