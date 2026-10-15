package didatrade.server;

import didatrade.*;
import io.grpc.*;
import junit.framework.TestCase;

import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static didatrade.server.AuditTestSupport.*;

/** Seven real gRPC peers; component schedules and autonomous C/Multi recovery. */
public class StateTransferRegressionTest extends TestCase {
    public void testInitialTransferDuplicateReplayAndIdleDelivery() throws Exception { isolated(getClass(), "initial"); }
    public void testTerminalFailureAutomaticallyRetriesWithForkedContext() throws Exception { isolated(getClass(), "failure"); }
    public void testEmptyPrefixGrowsAfterRecoveredDecision() throws Exception { isolated(getClass(), "empty"); }
    public void testPartialPrefixGrowsAfterMissingPayloadArrives() throws Exception { isolated(getClass(), "partial"); }
    public void testLostAcknowledgementAndConcurrentPrefixUpdatesStaySerialized() throws Exception { isolated(getClass(), "lost-ack"); }
    public void testFreezeRetainsOneCallAndUnfreezeTransfersLatestPrefix() throws Exception { isolated(getClass(), "freeze"); }
    public void testPermanentErrorsBackOffAndChannelShutdownStopsRetries() throws Exception { isolated(getClass(), "shutdown"); }
    public void testSevenWorkersChangeMembershipAndReconstructCausalHistory() throws Exception { isolated(getClass(), "live"); }

    public static void main(String[] args) {
        try {
            switch (args[0]) {
                case "initial" -> initial();
                case "failure" -> failure();
                case "empty" -> latePrefix(false);
                case "partial" -> latePrefix(true);
                case "lost-ack" -> lostAcknowledgement();
                case "freeze" -> freeze();
                case "shutdown" -> shutdown();
                case "live" -> live();
                default -> throw new IllegalArgumentException(args[0]);
            }
            System.exit(0);
        } catch (Throwable error) { error.printStackTrace(); System.exit(1); }
    }

    private static void initial() throws Exception {
        try (Cluster c = new Cluster(false, false, null)) {
            c.seed(0, 101, 42, 40);
            c.states[3].state_transfer.sendDecidedLog(2);
            await("initial replay executes", () -> c.states[6].getAppliedVersion() == 1);
            assertEquals(40, c.read(6, 42, 1).getBalance());
            // Force duplicate wire delivery, not merely sender-side suppression.
            var entry = DidaTradePaxos.DecidedEntry.newBuilder().setInstance(1).setBallot(2)
                    .setReqid(101).setAction(DidaTradeAction.ADDUSER.ordinal()).setUserid(42).setQuantity(40).setStock(20);
            var repeated = DidaTradePaxos.StateTransferRequest.newBuilder().setBallot(2).setServerid(3).addEntries(entry).build();
            c.paxos(6).statetransfer(repeated);
            c.paxos(6).statetransfer(repeated);
            await("duplicate position consumed", () -> cursor(c.states[6]) == 2);
            assertEquals(1L, c.states[6].getAppliedVersion());
            int calls = c.transfers.get();
            for (int i = 0; i < 20; i++) c.states[3].state_transfer.sendDecidedLog(2);
            Thread.sleep(600);
            assertEquals("unchanged acknowledged prefix must stay idle", calls, c.transfers.get());
        }
    }

    private static void failure() throws Exception {
        AtomicBoolean reject = new AtomicBoolean(true);
        try (Cluster c = new Cluster(false, false, reject(reject))) {
            c.seed(0, 101, 42, 40);
            Context.CancellableContext cancelled = Context.current().withCancellation();
            cancelled.cancel(null);
            cancelled.run(() -> c.states[3].state_transfer.sendDecidedLog(2));
            await("terminal failed transfer dispatched", () -> c.transfers.get() >= 1);
            assertEquals(0L, c.states[6].getAppliedVersion());
            reject.set(false); // No wakeup, new ballot or manual replay after recovery.
            await("automatic replay after terminal failure", () -> c.states[6].getAppliedVersion() == 1);
            assertEquals(40, c.read(6, 42, 1).getBalance());
            int calls = c.transfers.get(); Thread.sleep(700);
            assertEquals("retry stops after success", calls, c.transfers.get());
        }
    }

    private static void latePrefix(boolean partial) throws Exception {
        try (Cluster c = new Cluster(true, false, null)) {
            if (partial) c.seed(0, 101, 42, 40);
            int position = partial ? 1 : 0;
            int id = partial ? 201 : 101;
            int user = partial ? 43 : 42;
            for (int acceptor = 0; acceptor < 3; acceptor++)
                assertTrue(c.states[acceptor].acceptPhaseTwo(position, 0, id).accepted());
            if (!partial) c.payload(3, id, user, 40);
            c.states[3].setCurrentBallot(2); c.states[3].main_loop.wakeup();
            if (partial) {
                await("initial prefix transferred", () -> c.states[6].getAppliedVersion() == 1);
                await("later decision exists without payload", () -> c.states[3].paxos_log.isDecided(1));
                Future<DidaTradeMain.BalanceReply> read = c.pool.submit(() -> c.read(6, user, 2));
                Thread.sleep(150); assertFalse("causal read must wait for second execution", read.isDone());
                assertTrue(c.add(3, id, user, 40).getResult()); // Production registration wakes existing loop.
                assertEquals(40, read.get(8, TimeUnit.SECONDS).getBalance());
            }
            long expected = partial ? 2 : 1;
            await("recovered prefix transfers after preparation", () -> c.states[6].getAppliedVersion() == expected);
            assertEquals(expected, c.read(6, user, expected).getAppliedVersion());
            assertEquals(40, c.states[6].trade_manager.balance(user));
        }
    }

    private static void lostAcknowledgement() throws Exception {
        AtomicBoolean loseAck = new AtomicBoolean(true);
        ServerInterceptor lost = new ServerInterceptor() {
            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(ServerCall<Req, Resp> call,
                    Metadata headers, ServerCallHandler<Req, Resp> next) {
                if (!call.getMethodDescriptor().getFullMethodName().endsWith("/statetransfer") || !loseAck.getAndSet(false))
                    return next.startCall(call, headers);
                return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    // Apply the transfer normally, but make its acknowledgement fail.
                    @Override public void sendMessage(Resp response) { }
                    @Override public void close(Status status, Metadata trailers) {
                        super.close(Status.UNAVAILABLE.withDescription("lost transfer ACK"), trailers);
                    }
                }, headers);
            }
        };
        try (Cluster c = new Cluster(false, false, lost)) {
            c.seed(0, 101, 42, 40); c.states[3].state_transfer.sendDecidedLog(2);
            await("receiver applied before lost ACK", () -> c.states[6].getAppliedVersion() == 1);
            c.seed(1, 201, 43, 25);
            List<Future<?>> calls = new ArrayList<>();
            // Concurrent offers must share one delivery and coalesce prefix growth.
            for (int i = 0; i < 32; i++) calls.add(c.pool.submit(() -> c.states[3].state_transfer.sendDecidedLog(2)));
            for (Future<?> call : calls) call.get(3, TimeUnit.SECONDS);
            await("retry uses latest prefix", () -> c.states[6].getAppliedVersion() == 2);
            assertEquals(40, c.read(6, 42, 2).getBalance());
            assertEquals(25, c.read(6, 43, 2).getBalance());
            assertEquals(1, c.maxActive.get());
            int attempts = c.transfers.get(); Thread.sleep(700);
            assertEquals("no duplicate delivery loop", attempts, c.transfers.get());
        }
    }

    private static void freeze() throws Exception {
        try (Cluster c = new Cluster(false, false, null)) {
            c.states[6].freeze(); c.seed(0, 101, 42, 40);
            c.states[3].state_transfer.sendDecidedLog(2);
            await("one frozen handler", () -> c.transfers.get() == 1);
            c.seed(1, 201, 43, 25); c.states[3].state_transfer.sendDecidedLog(2);
            Thread.sleep(5300); // Longer than the old five-second transfer deadline.
            assertEquals("freeze must not accumulate cancelled handlers", 1, c.transfers.get());
            c.states[6].unfreeze();
            await("unfreeze drains latest prefix", () -> c.states[6].getAppliedVersion() == 2);
            assertEquals(1, c.maxActive.get());
            assertEquals(25, c.read(6, 43, 2).getBalance());
        }
    }

    private static void shutdown() throws Exception {
        AtomicBoolean reject = new AtomicBoolean(true);
        try (Cluster c = new Cluster(false, false, reject(reject))) {
            c.seed(0, 101, 42, 40); c.states[3].state_transfer.sendDecidedLog(2);
            await("bounded scheduled retries", () -> c.transfers.get() >= 4);
            for (int i = 1; i < 4; i++) {
                long gap = TimeUnit.NANOSECONDS.toMillis(c.attemptTimes.get(i) - c.attemptTimes.get(i - 1));
                assertTrue("backoff must not spin: " + gap, gap >= 200L * (1L << (i - 1)));
            }
            assertEquals(1, c.maxActive.get());
            c.states[3].channels[6].shutdownNow();
            await("shutdown releases delivery", () -> !hasDelivery(c.states[3].state_transfer, 6));
            int attempts = c.transfers.get(); Thread.sleep(500);
            assertEquals(attempts, c.transfers.get());
            assertEquals(0L, c.states[6].getAppliedVersion());
        }
    }

    private static void live() throws Exception {
        // Keep this learner behind until reconfiguration; progress delivery is now
        // also allowed to repair it during ballot 0.
        ServerInterceptor oldTransfers = new ServerInterceptor() {
            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(ServerCall<Req, Resp> call,
                    Metadata headers, ServerCallHandler<Req, Resp> next) {
                var listener = next.startCall(call, headers);
                if (!call.getMethodDescriptor().getFullMethodName().endsWith("/statetransfer")) return listener;
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                    private boolean rejected;
                    @Override public void onMessage(Req message) {
                        rejected = ((DidaTradePaxos.StateTransferRequest) message).getBallot() < 2;
                        if (rejected) call.close(Status.UNAVAILABLE.withDescription("old-ballot transfer withheld"), new Metadata());
                        else super.onMessage(message);
                    }
                    @Override public void onHalfClose() {
                        if (!rejected) super.onHalfClose();
                    }
                };
            }
        };
        try (Cluster c = new Cluster(true, true, oldTransfers)) {
            c.broadcast(101, 42, 40, 0, 1, 2, 3, 4);
            await("learners missing only payload", () -> c.states[5].paxos_log.isDecided(0) && c.states[6].paxos_log.isDecided(0));
            assertEquals(0L, c.states[6].getAppliedVersion());
            c.states[0].freeze(); c.states[1].freeze();
            Future<?> change = c.pool.submit(() -> DidaTradeMasterServiceGrpc.newBlockingStub(c.states[3].channels[3])
                    .withDeadlineAfter(12, TimeUnit.SECONDS).newballot(DidaTradeMaster.NewBallotRequest.newBuilder().setNewballot(2).build()));
            await("new acceptors caught up", () -> c.states[5].getAppliedVersion() == 1 && c.states[6].getAppliedVersion() == 1);
            c.broadcast(201, 43, 25, 2, 3, 4, 5, 6); change.get(12, TimeUnit.SECONDS);
            for (int replica = 2; replica < 7; replica++) {
                assertEquals(40, c.read(replica, 42, 2).getBalance());
                assertEquals(25, c.read(replica, 43, 2).getBalance());
                assertEquals(2L, c.states[replica].getAppliedVersion());
                assertEquals(Integer.valueOf(101), c.states[replica].paxos_log.getDecidedCommandId(0));
                assertEquals(Integer.valueOf(201), c.states[replica].paxos_log.getDecidedCommandId(1));
            }
        }
    }

    private static int cursor(DidaTradeServerState state) {
        try {
            Field field = MultiPaxosMainLoop.class.getDeclaredField("next_to_execute"); field.setAccessible(true);
            return field.getInt(state.main_loop);
        } catch (ReflectiveOperationException error) { throw new AssertionError(error); }
    }
    private static boolean hasDelivery(StateTransfer transfer, int replica) {
        try {
            Field field = StateTransfer.class.getDeclaredField("deliveries"); field.setAccessible(true);
            synchronized (transfer) { return ((Map<?, ?>) field.get(transfer)).containsKey(replica); }
        } catch (ReflectiveOperationException error) { throw new AssertionError(error); }
    }
    private static ServerInterceptor reject(AtomicBoolean reject) {
        return new ServerInterceptor() {
            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(ServerCall<Req, Resp> call,
                    Metadata headers, ServerCallHandler<Req, Resp> next) {
                if (!call.getMethodDescriptor().getFullMethodName().endsWith("/statetransfer") || !reject.get())
                    return next.startCall(call, headers);
                call.request(1);
                return new ServerCall.Listener<>() {
                    @Override public void onHalfClose() { call.close(Status.UNAVAILABLE.withDescription("transient transfer failure"), new Metadata()); }
                };
            }
        };
    }

    private static final class Cluster implements AutoCloseable {
        final DidaTradeServerState[] states = new DidaTradeServerState[7];
        final Server[] servers = new Server[7];
        final ExecutorService pool = Executors.newCachedThreadPool();
        final AtomicInteger transfers = new AtomicInteger(), active = new AtomicInteger(), maxActive = new AtomicInteger();
        final List<Long> attemptTimes = new CopyOnWriteArrayList<>();
        Cluster(boolean leaderWorker, boolean allWorkers, ServerInterceptor fault) throws Exception {
            int base = ports();
            try {
                for (int id = 0; id < 7; id++) {
                    states[id] = new DidaTradeServerState(base, id, 'C', ProtocolMode.MULTI,
                            allWorkers || id == 6 || id == 3 && leaderWorker);
                    ServerBuilder<?> builder = ServerBuilder.forPort(base + id)
                            .addService(new DidaTradeMainServiceImpl(states[id]))
                            .addService(new DidaTradePaxosServiceImpl(states[id]))
                            .addService(new DidaTradeMasterServiceImpl(states[id]));
                    if (id == 6) {
                        if (fault != null) builder.intercept(fault);
                        builder.intercept(new ServerInterceptor() {
                            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(ServerCall<Req, Resp> call,
                                    Metadata headers, ServerCallHandler<Req, Resp> next) {
                                if (call.getMethodDescriptor().getFullMethodName().endsWith("/statetransfer")) transfers.incrementAndGet();
                                return next.startCall(call, headers);
                            }
                        });
                    }
                    servers[id] = builder.build().start();
                }
                states[3].async_stubs[6] = states[3].async_stubs[6].withInterceptors(new ClientInterceptor() {
                    @Override public <Req, Resp> ClientCall<Req, Resp> interceptCall(MethodDescriptor<Req, Resp> method,
                            CallOptions options, Channel next) {
                        var call = next.newCall(method, options);
                        if (!method.getFullMethodName().endsWith("/statetransfer")) return call;
                        return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
                            @Override public void start(Listener<Resp> listener, Metadata headers) {
                                attemptTimes.add(System.nanoTime()); maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                                    @Override public void onClose(Status status, Metadata trailers) {
                                        active.decrementAndGet(); super.onClose(status, trailers);
                                    }
                                }, headers);
                            }
                        };
                    }
                });
            } catch (Throwable error) { close(); throw error; }
        }
        void payload(int replica, int reqid, int user, int cash) {
            states[replica].req_history.addToPending(reqid, new RequestRecord(reqid, new DidaTradeCommand(DidaTradeAction.ADDUSER, user, cash, 20)));
        }
        void seed(int position, int id, int user, int cash) {
            payload(3, id, user, cash); states[3].paxos_log.decideEntry(position, 0, id);
            states[3].executeRequest(states[3].req_history.getIfPending(id));
        }
        DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub main(int replica) {
            return DidaTradeMainServiceGrpc.newBlockingStub(states[3].channels[replica]).withDeadlineAfter(8, TimeUnit.SECONDS);
        }
        DidaTradePaxosServiceGrpc.DidaTradePaxosServiceBlockingStub paxos(int replica) {
            return DidaTradePaxosServiceGrpc.newBlockingStub(states[3].channels[replica]).withDeadlineAfter(3, TimeUnit.SECONDS);
        }
        DidaTradeMain.AddUserReply add(int replica, int id, int user, int cash) {
            return main(replica).adduser(DidaTradeMain.AddUserRequest.newBuilder().setReqid(id).setUserid(user).setCash(cash).setStock(20).build());
        }
        DidaTradeMain.BalanceReply read(int replica, int user, long version) {
            return main(replica).balance(DidaTradeMain.BalanceRequest.newBuilder().setUserid(user).setMinVersion(version).build());
        }
        void broadcast(int id, int user, int cash, int... replicas) throws Exception {
            List<Future<DidaTradeMain.AddUserReply>> replies = new ArrayList<>();
            for (int replica : replicas) replies.add(pool.submit(() -> add(replica, id, user, cash)));
            for (var reply : replies) assertTrue(reply.get(12, TimeUnit.SECONDS).getResult());
        }
        @Override public void close() {
            pool.shutdownNow();
            for (var state : states) if (state != null) for (var channel : state.channels) channel.shutdownNow();
            for (var server : servers) if (server != null) server.shutdownNow();
        }
        static int ports() throws Exception {
            for (int attempt = 0; attempt < 100; attempt++) {
                List<ServerSocket> sockets = new ArrayList<>();
                try {
                    ServerSocket first = new ServerSocket(0); sockets.add(first); int base = first.getLocalPort();
                    if (base + 6 > 65535) continue;
                    for (int i = 1; i < 7; i++) sockets.add(new ServerSocket(base + i));
                    return base;
                } catch (java.io.IOException occupied) { }
                finally { for (var socket : sockets) socket.close(); }
            }
            throw new IllegalStateException("No free seven-port range");
        }
    }
}
