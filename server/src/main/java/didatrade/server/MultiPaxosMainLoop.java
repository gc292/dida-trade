package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiPaxosMainLoop implements ConsensusLoop {
    private final DidaTradeServerState                       server_state;
    private       int                                        prepared_ballot  = -1;
    private       boolean                                    has_work;
    private       Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries = new HashMap<>();
    private int next_to_execute = 0;


    public MultiPaxosMainLoop(DidaTradeServerState state) {
        server_state = state;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            int ballot = server_state.getCurrentBallot();
            boolean is_leader = server_state.scheduler.leader(ballot) == server_state.my_id;
            
            if (is_leader && prepared_ballot != ballot) {
                boolean prepared = prepareLeadership(ballot);
                if (!prepared ) {
                    if (server_state.getCurrentBallot() != ballot) continue;
                    synchronized (this) {
                        try { wait(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

                    }
                    continue;
                }
            } 

            PaxosInstance slot_entry = server_state.paxos_log.testAndSetEntry(next_to_execute);

            if (!slot_entry.decided){
                if (is_leader && prepared_ballot == ballot) {
                    int value;
                    DidaTradePaxos.AcceptedEntry recovered = recoveredEntries.get(next_to_execute);

                    if (recovered !=null) {
                        value = recovered.getValue();
                    } else {
                        RequestRecord req = server_state.req_history.getFirstPending();
                        if (req == null) {
                            waitForRequest();
                            continue;
                        }
                        value = req.getId();

                    }

                    boolean success = runPhaseTwo(next_to_execute, ballot, value);
                    if (!success) continue;

                    slot_entry.command_id = value;
                    slot_entry.decided = true;
                    recoveredEntries.remove(next_to_execute);
                    server_state.setCompletedBallot(ballot);

                } else {
                    waitForRequest();
                    continue;
                }
            }

            RequestRecord req = server_state.req_history.getIfPending(slot_entry.command_id);
            while (req == null){
                waitForRequest();
                req = server_state.req_history.getIfPending(slot_entry.command_id);
            }
            server_state.executeRequest((req));
            next_to_execute++;

        }
    }

    @Override
    public synchronized void wakeup() {
        has_work = true;
        notifyAll();
    }

    private void waitForRequest() {
        synchronized(this) {
               while (!has_work && !Thread.currentThread().isInterrupted()) {
                    try { wait(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
            has_work = false;
        }
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

    private boolean runPhaseTwo(int slot, int ballot, int value) {
        List<Integer>  acceptors   = server_state.scheduler.acceptors(ballot);
        int            n_acceptors = acceptors.size();
        int            quorum      = server_state.scheduler.quorum(ballot);

        DidaTradePaxos.PhaseTwoRequest request = DidaTradePaxos.PhaseTwoRequest
        .newBuilder()
        .setInstance(slot)
        .setRequestballot(ballot)
        .setValue(value)
        .build();

        PhaseTwoResponseProcessor                              processor = new PhaseTwoResponseProcessor(quorum);
        ArrayList<DidaTradePaxos.PhaseTwoReply>                responses = new ArrayList<>();
        GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(responses, n_acceptors, processor);
        
        for (int i = 0; i < n_acceptors; i++) {
            server_state.async_stubs[acceptors.get(i)].phasetwo(
                    request, new CollectorStreamObserver<>(collector));
        }

        collector.waitUntilDone();

        if (!processor.getAccepted()) {
            server_state.setCurrentBallot(processor.getMaxballot());
            return false;
        }
        
        return true;

    }
}
