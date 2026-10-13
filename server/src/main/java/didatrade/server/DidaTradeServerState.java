package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradeMaster;
import didatrade.DidaTradeMasterServiceGrpc;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.configs.ConfigurationScheduler;
import didatrade.core.TradeManager;
import didatrade.util.*;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class DidaTradeServerState {
    public static final  int  DEFAULT_POPULATION            = 10;
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
    private final boolean fastpaxos_on;

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
        this(port, myself, schedule, protocol, true);
    }

    // Component tests drive the selected loop explicitly without changing the protocol mode.
    DidaTradeServerState(int port, int myself, char schedule, ProtocolMode protocol, boolean startWorker) {
        if (protocol == ProtocolMode.FAST && schedule != 'B') {
            throw new IllegalArgumentException("Fast Paxos requires Schedule B");
        }
        this.trade_manager    = new TradeManager();
        this.scheduler        = new ConfigurationScheduler(schedule);
        this.base_port        = port;
        this.my_id            = myself;
        this.debug_mode       = 0;
        this.fastpaxos_on     = protocol == ProtocolMode.FAST;
        this.current_ballot   = 0;
        this.completed_ballot = -1;
        this.req_history      = new RequestHistory();
        this.paxos_log        = new PaxosLog();
        this.slowMode         = false;
        this.frozen           = false;

        this.main_loop = switch (protocol) {
            case CLASSIC -> new MainLoop(this);
            case MULTI -> new MultiPaxosMainLoop(this);
            case FAST -> new FastPaxosMainLoop(this, 4);
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
        if (startWorker) {
            this.main_loop_worker = new Thread(main_loop);
            this.main_loop_worker.start();
        }
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

    public int getFastQuorum(int ballot) {
        return scheduler.superMajority(ballot);
    }

    public FastPhaseOneResponseProcessor runFastPhaseOne(int ballot) {
        List<Integer> acceptors = scheduler.acceptors(ballot);

        DidaTradePaxos.PhaseOneRequest request = DidaTradePaxos.PhaseOneRequest.newBuilder().setInstance(0).setRequestballot(ballot).build();

        FastPhaseOneResponseProcessor processor = new FastPhaseOneResponseProcessor(ballot, acceptors, scheduler.recoveryQuorum(ballot), scheduler.superMajority(ballot));

        ArrayList<DidaTradePaxos.PhaseOneReply> responses = new ArrayList<>();

        GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector = new GenericResponseCollector<>(responses, acceptors.size(), processor);

        for (int acceptor : acceptors) {
            CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> observer = new CollectorStreamObserver<>(collector);

            async_stubs[acceptor]
                    .withDeadlineAfter(CONSENSUS_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .phaseone(request, observer);
        }
        collector.waitUntilDone();
        return processor;
    }

    public PhaseTwoAttempt startFastPhaseTwo(int instance, int ballot, int value) {
        return startPhaseTwo(instance, ballot, value, getFastQuorum(ballot));
    }

    public PhaseTwoAttempt startFastAny(int instance, int ballot) {
        DidaTradePaxos.PhaseTwoRequest request =
                DidaTradePaxos.PhaseTwoRequest.newBuilder()
                        .setInstance(instance)
                        .setRequestballot(ballot)
                        .setAny(true)
                        .build();

        return sendPhaseTwo(request, getFastQuorum(ballot));
    }


    private PhaseTwoAttempt sendPhaseTwo(
            DidaTradePaxos.PhaseTwoRequest request, int quorum) {

        List<Integer> acceptors = scheduler.acceptors(request.getRequestballot());

        PhaseTwoResponseProcessor               processor = new PhaseTwoResponseProcessor(quorum);
        ArrayList<DidaTradePaxos.PhaseTwoReply> responses = new ArrayList<>();

        GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector =
                new GenericResponseCollector<>(responses, acceptors.size(), processor);

        for (int acceptor : acceptors) {
            CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> observer =
                    new CollectorStreamObserver<>(collector) {
                        private void authorizationFailed() {
                            if (request.getAny() && main_loop instanceof FastPaxosMainLoop loop) {
                                loop.onAuthorizationFailure(request.getInstance(), request.getRequestballot());
                            }
                        }
                        @Override public void onNext(DidaTradePaxos.PhaseTwoReply reply) {
                            if (!reply.getAccepted()) authorizationFailed();
                            super.onNext(reply);
                        }
                        @Override public void onError(Throwable error) {
                            authorizationFailed();
                            super.onError(error);
                        }
                    };

            DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub stub = async_stubs[acceptor]
                    .withDeadlineAfter(CONSENSUS_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (request.getAny()) stub = stub.withWaitForReady();
            stub.phasetwo(request, observer);
        }

        return new PhaseTwoAttempt(collector, processor);
    }

    public synchronized PhaseTwoAcceptorResult authorizeFastInstance(int instance, int ballot) {
        if (ballot < current_ballot) {
            return new PhaseTwoAcceptorResult(false, current_ballot);
        }

        current_ballot = ballot;
        boolean accepted = paxos_log.authorizeFastInstance(instance, ballot);
        return new PhaseTwoAcceptorResult(accepted, current_ballot);
    }

    public synchronized PhaseTwoAcceptorResult acceptFastProposal(int instance, int ballot, int value) {
        if (frozen || ballot != current_ballot) {
            return new PhaseTwoAcceptorResult(false, current_ballot);
        }

        boolean accepted = paxos_log.acceptFastProposal(instance, ballot, value);
        return new PhaseTwoAcceptorResult(accepted, current_ballot);
    }


    public PhaseTwoAttempt startFastPhaseTwoIfCurrentAndUndecided(
            int instance, int ballot, int value) {
        return startIfCurrentAndUndecided(
                instance, ballot, () -> startFastPhaseTwo(instance, ballot, value));
    }

    public PhaseTwoAttempt startFastAnyIfCurrentAndUndecided(int instance, int ballot) {
        return startIfCurrentAndUndecided(
                instance, ballot, () -> startFastAny(instance, ballot));
    }

    /**
     * Sends a concrete vote outside acceptor locks and the originating RPC cancellation.
     */
    public void notifyLearners(int instance, int ballot, int value) {
        Context ctx = Context.current().fork();
        ctx.run(() -> {
            List<Integer> learners  = this.scheduler.learners(ballot);
            int           n_targets = learners.size();

            DidaTradePaxos.LearnRequest.Builder learn_request_builder = DidaTradePaxos.LearnRequest.newBuilder();
            learn_request_builder.setInstance(instance);
            learn_request_builder.setValue(value);
            learn_request_builder.setBallot(ballot);
            learn_request_builder.setServerid(this.my_id);

            DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build();

            // System.out.println("Sending learn request: \n" + learn_request);

            System.out.println("Paxos acceptor: going to notify learners for entry " + instance + " with timestamp " + ballot + " request = " + learn_request);
            ArrayList<DidaTradePaxos.LearnReply>                learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
            GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);
            for (int i = 0; i < n_targets; i++) {
                CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
                DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub learnerStub = this.async_stubs[learners.get(i)];
                // A startup connection failure must not permanently lose a Fast vote.
                if (getFastPaxosMode()) learnerStub = learnerStub.withWaitForReady();
                learnerStub.learn(learn_request, learn_observer);
            }
            // System.out.println("Learn request completed for instance = " + instance);
        });
    }

    public PhaseTwoResponseProcessor runPhaseTwo(int instance, int ballot, int value) {
        PhaseTwoAttempt attempt = startPhaseTwo(instance, ballot, value);
        System.out.println("Waiting for responses...");
        return attempt.waitUntilDone();
    }

    public PhaseTwoAttempt startPhaseTwo(int instance, int ballot, int value) {
        return startPhaseTwo(instance, ballot, value, scheduler.quorum(ballot));
    }

    private PhaseTwoAttempt startPhaseTwo(
            int instance, int ballot, int value, int quorum) {

        DidaTradePaxos.PhaseTwoRequest request =
                DidaTradePaxos.PhaseTwoRequest.newBuilder()
                        .setInstance(instance)
                        .setRequestballot(ballot)
                        .setValue(value)
                        .build();

        return sendPhaseTwo(request, quorum);
    }

    public PhaseTwoAttempt startPhaseTwoIfCurrentAndUndecided(
            int instance, int ballot, int value) {
        return startIfCurrentAndUndecided(
                instance, ballot, () -> startPhaseTwo(instance, ballot, value));
    }

    private synchronized PhaseTwoAttempt startIfCurrentAndUndecided(
            int instance, int ballot, Supplier<PhaseTwoAttempt> starter) {
        if (ballot != current_ballot) return null;
        return paxos_log.startIfUndecided(instance, starter);
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

    public void setCurrentBallot(int ballot) {
        boolean wake;
        synchronized (this) {
            wake = ballot > current_ballot;
            if (wake) current_ballot = ballot;
            wake = wake && fastpaxos_on;
        }
        if (wake) main_loop.wakeup();
    }

    synchronized boolean advanceFastBallotIfCurrent(int ballot) {
        if (current_ballot != ballot) return false;
        current_ballot = ballot + 1;
        return true;
    }

    void notifyFastLeader(int ballot, Consumer<Boolean> completion) {
        DidaTradeMaster.NewBallotRequest request = DidaTradeMaster.NewBallotRequest.newBuilder()
                .setNewballot(ballot).setCompletedballot(getCompletedBallot()).build();
        DidaTradeMasterServiceGrpc.newStub(channels[scheduler.leader(ballot)])
                .withDeadlineAfter(CONSENSUS_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .newballot(request, new StreamObserver<>() {
                    @Override public void onNext(DidaTradeMaster.NewBallotReply reply) {}
                    @Override public void onError(Throwable error) { completion.accept(false); }
                    @Override public void onCompleted() { completion.accept(true); }
                });
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

    public boolean getFastPaxosMode() {
        return this.fastpaxos_on;
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
        if (fastpaxos_on) main_loop.wakeup();
    }
}
