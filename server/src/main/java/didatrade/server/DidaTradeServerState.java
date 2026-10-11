package didatrade.server;

import java.util.ArrayList;

import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import didatrade.util.PhaseTwoAttempt;
import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.configs.ConfigurationScheduler;
import didatrade.core.TradeManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


public class DidaTradeServerState {
    public static final int DEFAULT_POPULATION = 10;
    private static final long CONSENSUS_RPC_TIMEOUT_SECONDS = 2;
    int                    max_participants;
    TradeManager           trade_manager;
    ConfigurationScheduler scheduler;
    int                    base_port;
    int                    my_id;
    RequestHistory         req_history;
    PaxosLog               paxos_log;

    List<Integer>                                         all_participants;
    int                                                   n_participants;
    String[]                                              targets;
    ManagedChannel[]                                      channels;
    DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;

    private int     current_ballot;
    private int     completed_ballot;
    private int     debug_mode;
    private boolean fastpaxos_on;

    private volatile boolean slowMode;
    private          boolean frozen;

    ConsensusLoop main_loop;
    Thread        main_loop_worker;

    record PhaseOneAcceptorResult(
            boolean accepted, int maxBallot, List<DidaTradePaxos.AcceptedEntry> acceptedEntries) {
    }

    record PhaseTwoAcceptorResult(boolean accepted, int maxBallot) {
    }

    public DidaTradeServerState(int port, int myself, char schedule, ProtocolMode protocol) {
        this.trade_manager    = new TradeManager();
        this.scheduler        = new ConfigurationScheduler(schedule);
        this.base_port        = port;
        this.my_id            = myself;
        this.debug_mode       = 0;
        this.fastpaxos_on     = false;
        this.current_ballot   = 0;
        this.completed_ballot = -1;
        this.req_history      = new RequestHistory();
        this.paxos_log        = new PaxosLog();
        this.slowMode         = false;
        this.frozen           = false;

        this.main_loop = switch (protocol) {
            case CLASSIC -> new MainLoop(this);
            case MULTI -> new MultiPaxosMainLoop(this);
        };

        // populate manager
        this.trade_manager.populate(DEFAULT_POPULATION);

        // init comms
        this.all_participants = this.scheduler.allparticipants();
        this.n_participants   = all_participants.size();

        this.targets = new String[this.n_participants];
        for (int i = 0; i < this.n_participants; i++) {
            int target_port = this.base_port + all_participants.get(i);
            this.targets[i] = new String();
            this.targets[i] = "localhost:" + target_port;
            System.out.printf("targets[%d] = %s%n", i, targets[i]);
        }

        this.channels = new ManagedChannel[this.n_participants];
        for (int i = 0; i < this.n_participants; i++)
             this.channels[i] = ManagedChannelBuilder.forTarget(this.targets[i]).usePlaintext().build();

        this.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[this.n_participants];
        for (int i = 0; i < this.n_participants; i++)
             this.async_stubs[i] = DidaTradePaxosServiceGrpc.newStub(this.channels[i]);

        // start worker
        this.main_loop_worker = new Thread(main_loop);
        this.main_loop_worker.start();
    }

    public PhaseOneResponseProcessor runPhaseOne(
            int entry_number, int ballot, int completed_ballot) {
        List<Integer> acceptors   = this.scheduler.acceptors(ballot);
        int           n_acceptors = acceptors.size();

        DidaTradePaxos.PhaseOneRequest.Builder phase_one_request_builder = DidaTradePaxos.PhaseOneRequest.newBuilder();
        phase_one_request_builder.setInstance(entry_number);
        phase_one_request_builder.setRequestballot(ballot);

        DidaTradePaxos.PhaseOneRequest phase_one_request = phase_one_request_builder.build();
        System.out.println("Going to send phase 1" + phase_one_request);

        int low_ballot  = Math.max(completed_ballot, 0);
        int high_ballot = ballot;


        PhaseOneResponseProcessor phase_one_processor = new PhaseOneResponseProcessor(this.scheduler, low_ballot, high_ballot);

        ArrayList<DidaTradePaxos.PhaseOneReply>                phase_one_responses = new ArrayList<DidaTradePaxos.PhaseOneReply>();
        GenericResponseCollector<DidaTradePaxos.PhaseOneReply> phase_one_collector = new GenericResponseCollector<DidaTradePaxos.PhaseOneReply>(phase_one_responses, n_acceptors, phase_one_processor);

        for (int i = 0; i < n_acceptors; i++) {
            CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> phase_one_observer = new CollectorStreamObserver<DidaTradePaxos.PhaseOneReply>(phase_one_collector);
            this.async_stubs[acceptors.get(i)]
                    .withDeadlineAfter(CONSENSUS_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .phaseone(phase_one_request, phase_one_observer);
        }


        phase_one_collector.waitUntilDone();
        return phase_one_processor;
    }

    public PhaseTwoResponseProcessor runPhaseTwo(int instance, int ballot, int value) {
        PhaseTwoAttempt attempt = startPhaseTwo(instance, ballot, value);
        System.out.println("Waiting for responses...");
        return attempt.waitUntilDone();
    }

    public PhaseTwoAttempt startPhaseTwo(int instance, int ballot, int value) {
        List<Integer> acceptors   = this.scheduler.acceptors(ballot);
        int           quorum      = this.scheduler.quorum(ballot);
        int           n_acceptors = acceptors.size();

        DidaTradePaxos.PhaseTwoRequest.Builder phase_two_request = DidaTradePaxos.PhaseTwoRequest.newBuilder();
        phase_two_request.setInstance(instance);
        phase_two_request.setRequestballot(ballot);
        phase_two_request.setValue(value);


        PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);

        System.out.println("Calling peers with phase_two_request = " + phase_two_request);
        ArrayList<DidaTradePaxos.PhaseTwoReply>                phase_two_responses = new ArrayList<DidaTradePaxos.PhaseTwoReply>();
        GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> phase_two_collector = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor);
        for (int i = 0; i < n_acceptors; i++) {
            CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> phase_two_observer = new CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply>(phase_two_collector);
            this.async_stubs[acceptors.get(i)]
                    .withDeadlineAfter(CONSENSUS_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .phasetwo(phase_two_request.build(), phase_two_observer);
        }

        return new PhaseTwoAttempt(phase_two_collector, phase_two_processor);
    }

    public synchronized PhaseTwoAttempt startPhaseTwoIfCurrentAndUndecided(
            int instance, int ballot, int value) {
        if (ballot != this.current_ballot) return null;

        return this.paxos_log.startIfUndecided(
                instance, () -> startPhaseTwo(instance, ballot, value));
    }

    public synchronized PhaseOneAcceptorResult prepareAcceptor(int ballot) {
        boolean accepted = ballot >= this.current_ballot;

        if (accepted) this.current_ballot = ballot;

        List<DidaTradePaxos.AcceptedEntry> entries = accepted
                ? this.paxos_log.getAcceptedEntries()
                : List.of();
        return new PhaseOneAcceptorResult(accepted, this.current_ballot, entries);
    }

    public synchronized PhaseTwoAcceptorResult acceptPhaseTwo(int instance, int ballot, int value) {
        if (ballot < this.current_ballot) {
            return new PhaseTwoAcceptorResult(false, this.current_ballot);
        }

        this.current_ballot = ballot;
        boolean accepted = this.paxos_log.accept(instance, ballot, value);
        return new PhaseTwoAcceptorResult(accepted, this.current_ballot);
    }

    public void executeRequest(RequestRecord request_record) {
        DidaTradeCommand command = request_record.getRequest();
        boolean          result  = false;
        int              balance = 0;

        DidaTradeAction action = command.getAction();

        // System.out.println("Action  = " + action);
        switch (action) {
            case DidaTradeAction.POPULATE:
                result = this.trade_manager.populate(command.getQuantity());
                break;
            case DidaTradeAction.ADDUSER:
                result = this.trade_manager.add_user(command.getUserId(), command.getQuantity(), command.getStock());
                break;
            case DidaTradeAction.SELL:
                result = this.trade_manager.sell(command.getUserId(), command.getQuantity());
                break;
            case DidaTradeAction.BUY:
                result = this.trade_manager.acquire(command.getUserId(), command.getQuantity());
                break;
            case DidaTradeAction.BALANCE:
                balance = this.trade_manager.balance(command.getUserId());
                command.setQuantity(balance);
                result = (balance != -1);
                break;
            case DidaTradeAction.DUMP:
                this.trade_manager.dump();
                result = true;
                break;
            default:
                result = false;
                System.err.println("*** Unknown command ****");
                break;
        }

        // sending response
        System.out.println("Setting response for command with id = " + request_record.getId() + " with result = " + result);
        request_record.setResponse(result);
        this.req_history.moveToProcessed(request_record.getId());
    }

    public synchronized int getCurrentBallot() {
        return this.current_ballot;
    }

    public synchronized void setCurrentBallot(int ballot) {
        if (ballot > this.current_ballot) this.current_ballot = ballot;
    }

    public synchronized int getCompletedBallot() {
        return this.completed_ballot;
    }

    public int findMaxDecidedBallot() {
        return this.paxos_log.findMaxDecidedBallot();
    }

    public synchronized void updateCompletedBallot(int ballot) {
        // WARNING: THIS ONLY WORKS FOR CONFIGURATIONS WHERE THERE IS NO NEED FOR STATE-TRANSFER!!!!!
        // NEEDS TO BE UPDATE FOR THE PROJECT


        ballot = this.findMaxDecidedBallot();
        if (ballot > this.completed_ballot) this.completed_ballot = ballot;
        this.notifyAll();
    }


    public synchronized void setCompletedBallot(int ballot) {
        if (ballot > this.completed_ballot) this.completed_ballot = ballot;
        this.notifyAll();
    }


    public synchronized int waitForCompletedBallot(int ballot) {
        while (this.completed_ballot < ballot) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
        return this.completed_ballot;
    }

    public synchronized boolean getFastPaxosMode() {
        return this.fastpaxos_on;
    }

    public synchronized void setFastPaxosMode(boolean mode) {
        this.fastpaxos_on = mode;
    }

    public synchronized int getDebugMode() {
        return this.debug_mode;
    }

    public synchronized void setDebugMode(DebugMode mode) {
        Objects.requireNonNull(mode, "mode");
        switch (mode) {
            case CRASH -> crash();
            case FREEZE -> freeze();
            case UNFREEZE -> unfreeze();
            case SLOW_ON -> setSlowMode(true);
            case SLOW_OFF -> setSlowMode(false);
        }
        this.debug_mode = mode.getCode();
    }

    public void crash() {

        System.out.println("Crashing");
        System.exit(1);
    }

    public void setSlowMode(boolean enabled) {
        this.slowMode = enabled;
    }

    public boolean beforeRequest(io.grpc.stub.StreamObserver<?> responseObserver) {
        try {
            waitUntilUnfrozen();
            applySlowMode();
            waitUntilUnfrozen();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            responseObserver.onError(io.grpc.Status.CANCELLED.withDescription("Request interrupted while waiting").asRuntimeException());
            return false;
        }
    }

    public void applySlowMode() throws InterruptedException {
        if (slowMode) {
            int delay = ThreadLocalRandom.current().nextInt(100, 1001);
            Thread.sleep(delay);
        }
    }

    public synchronized void freeze() {
        this.frozen = true;
    }

    public synchronized void waitUntilUnfrozen() throws InterruptedException {
        while (this.frozen) {
            wait();
        }
    }

    public synchronized void unfreeze() {
        this.frozen = false;
        notifyAll();
    }
}
