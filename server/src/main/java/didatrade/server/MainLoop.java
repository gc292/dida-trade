package didatrade.server;

import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import io.grpc.ManagedChannel;

import java.util.List;


public class MainLoop implements ConsensusLoop {
    DidaTradeServerState server_state;

    private boolean                                               has_work;
    private int                                                   next_log_entry;
    private List<Integer>                                         all_participants;
    private int                                                   n_participants;
    private String[]                                              targets;
    private ManagedChannel[]                                      channels;
    private DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;


    public MainLoop(DidaTradeServerState state) {
        this.server_state   = state;
        this.has_work       = false;
        this.next_log_entry = -1;
    }

    public void run() {
        while (true) {
            this.next_log_entry++;
            this.processEntry(this.next_log_entry);
        }
    }

    public synchronized void wakeup() {
        this.has_work = true;
        notify();
    }


    public synchronized void processEntry(int entry_number) {

        PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);

        while (next_entry.decided == false) {
            RequestRecord request_record   = this.server_state.req_history.getFirstPending();
            int           ballot           = this.server_state.getCurrentBallot();
            int           completed_ballot = this.server_state.getCompletedBallot();

            if ((ballot > -1) && (request_record != null) && (this.server_state.scheduler.leader(ballot) == this.server_state.my_id)) {

                System.out.println("I am the leader for request with id " + request_record.getId());
                boolean ballot_aborted       = false;
                int     phase_one_readballot = -1;
                int     phase_two_value      = request_record.getId();

                // Paxos Phase One
                System.out.println("Going to run paxos phase 1");


                // send phase1
                PhaseOneResponseProcessor phase_one_processor =
                        this.server_state.runPhaseOne(entry_number, ballot, completed_ballot);
                if (phase_one_processor.getAccepted() == false) {
                    ballot_aborted = true;
                    int maxballot = phase_one_processor.getMaxballot();
                    if (maxballot > this.server_state.getCurrentBallot()) this.server_state.setCurrentBallot(maxballot);
                } else if (phase_one_processor.getValballot() > -1) phase_two_value = phase_one_processor.getValue();


                System.out.println("Paxos phase 1 ended with aborted = " + ballot_aborted + " and read ballot = " + phase_one_processor.getValballot() + " and value " + phase_two_value);


                // Paxos Phase Two
                if (ballot_aborted == false) {
                    System.out.println("Going to run paxos phase 2");

                    // send phase2
                    PhaseTwoResponseProcessor phase_two_processor =
                            this.server_state.runPhaseTwo(entry_number, ballot, phase_two_value);
                    if (phase_two_processor.getAccepted() == false) {
                        ballot_aborted = true;
                        this.server_state.setCurrentBallot(phase_two_processor.getMaxballot());
                    }
                    System.out.println("Paxos phase 2 ended with ballot_aborted = " + ballot_aborted);
                }

                // After phase2
                if (ballot_aborted == false) {
                    this.server_state.setCompletedBallot(ballot);
                    next_entry.command_id = phase_two_value;
                    next_entry.decided    = true;
                }
            }
            if (next_entry.decided == false) {
                System.out.println("Entry not decided: waiting");
                this.has_work = false;
                while (this.has_work == false) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                    }
                }
            }

        }


        System.out.println("Log entry with number " + this.next_log_entry + " has been decided with command id = " + next_entry.command_id);
        RequestRecord request_record = this.server_state.req_history.getIfPending(next_entry.command_id);
        // if I receive the paxos decision before the request
        while (request_record == null) {
            System.out.println("Record not available!");
            try {
                wait();
            } catch (InterruptedException e) {
            }
            request_record = this.server_state.req_history.getIfPending(next_entry.command_id);
        }

        // exec request in entry
        // System.out.println("Going to process command with id = " + next_entry.command_id);

        this.server_state.executeRequest(request_record);
    }
}
