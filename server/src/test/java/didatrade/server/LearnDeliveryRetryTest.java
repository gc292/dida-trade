package didatrade.server;

import didatrade.DidaTradePaxos;
import io.grpc.*;
import junit.framework.TestCase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static didatrade.server.AuditTestSupport.*;

/** Shared Learn delivery: real learner services, controlled lost ACKs, no mocked log. */
public class LearnDeliveryRetryTest extends TestCase {
    public void testLostAcknowledgementAndConcurrentDuplicateNotifications() throws Exception {
        isolated(getClass(), "lost-ack");
    }

    public void testChannelShutdownStopsRetryAndReleasesPendingVotes() throws Exception {
        isolated(getClass(), "shutdown");
    }

    public void testFrozenLearnerKeepsOneCallAndUnfreezeDrainsBacklog() throws Exception {
        isolated(getClass(), "freeze");
    }

    public static void main(String[] args) {
        try {
            if (args[0].equals("lost-ack")) lostAcknowledgement();
            else if (args[0].equals("shutdown")) shutdown();
            else if (args[0].equals("freeze")) freeze();
            else throw new IllegalArgumentException(args[0]);
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static void lostAcknowledgement() throws Exception {
        AtomicBoolean firstAck = new AtomicBoolean(true);
        List<DidaTradePaxos.LearnRequest> received = new CopyOnWriteArrayList<>();
        ServerInterceptor lostAck = acknowledgements(firstAck, false, received);
        try (Cluster c = new Cluster(ProtocolMode.MULTI, false, id -> id == 2 ? lostAck : null)) {
            DidaTradeServerState sender = c.states[0];
            AtomicInteger active = new AtomicInteger();
            AtomicInteger maxActive = new AtomicInteger();
            sender.async_stubs[2] = sender.async_stubs[2].withInterceptors(new ClientInterceptor() {
                @Override public <Req, Resp> ClientCall<Req, Resp> interceptCall(
                        MethodDescriptor<Req, Resp> method, CallOptions options, Channel next) {
                    ClientCall<Req, Resp> call = next.newCall(method, options);
                    if (!method.getFullMethodName().endsWith("/learn")) return call;
                    return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
                        @Override public void start(Listener<Resp> listener, Metadata headers) {
                            maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                                @Override public void onClose(Status status, Metadata trailers) {
                                    active.decrementAndGet();
                                    super.onClose(status, trailers);
                                }
                            }, headers);
                        }
                    };
                }
            });
            assertTrue(sender.acceptPhaseTwo(0, 0, 101).accepted());
            sender.notifyLearners(0, 0, 101);
            await("original vote processed with missing ACK", () -> !received.isEmpty());
            assertFalse("One acceptor cannot decide", c.states[2].paxos_log.isDecided(0));

            List<Future<?>> duplicates = new ArrayList<>();
            for (int i = 0; i < 32; i++)
                duplicates.add(c.pool.submit(() -> sender.notifyLearners(0, 0, 101)));
            for (Future<?> duplicate : duplicates) duplicate.get(3, TimeUnit.SECONDS);
            for (int position = 1; position <= 8; position++) {
                assertTrue(sender.acceptPhaseTwo(position, 0, 1000 + position).accepted());
                sender.notifyLearners(position, 0, 1000 + position);
            }
            sender.prepareAcceptor(3); // Pending old-ballot votes must not be discarded.
            await("retry and backlog acknowledged", () -> pendingEmpty(sender));
            await("all eight later votes reached learner", () -> received.stream()
                    .map(DidaTradePaxos.LearnRequest::getInstance).distinct().count() == 9);
            List<DidaTradePaxos.LearnRequest> repeated = received.stream()
                    .filter(request -> request.getInstance() == 0).toList();
            assertTrue("Lost ACK must cause redelivery", repeated.size() >= 2);
            for (DidaTradePaxos.LearnRequest request : repeated) assertEquals(repeated.get(0), request);
            for (DidaTradePaxos.LearnRequest request : received) {
                assertEquals(0, request.getBallot());
                assertEquals(0, request.getServerid());
                assertEquals(request.getInstance() == 0 ? 101 : 1000 + request.getInstance(), request.getValue());
            }
            assertEquals("Retries and concurrent notifications stay single-flight", 1, maxActive.get());
            assertEquals(0, active.get());
            assertEquals(3, sender.getCurrentBallot());
            assertFalse("Repeated deliveries do not manufacture a quorum", c.states[2].paxos_log.isDecided(0));
            DidaTradePaxos.AcceptedEntry original = sender.paxos_log.getAcceptedEntries().stream()
                    .filter(entry -> entry.getInstance() == 0).findFirst().orElseThrow();
            assertEquals(0, original.getValballot());
            assertEquals(101, original.getValue());

            assertTrue(c.states[1].acceptPhaseTwo(0, 0, 101).accepted());
            c.states[1].notifyLearners(0, 0, 101);
            await("second distinct vote forms decision", () -> c.states[2].paxos_log.isDecided(0));
            for (DidaTradeServerState state : c.states) {
                await("all learners agree", () -> state.paxos_log.isDecided(0));
                assertEquals(101, state.paxos_log.getDecidedCommandId(0).intValue());
            }
            System.out.println("LEARN retry lost ACK: identical redelivery, maxActive=" + maxActive
                    + ", queue empty, duplicate vote did not decide, later distinct vote decided 101");
        }
    }

    private static void shutdown() throws Exception {
        List<DidaTradePaxos.LearnRequest> received = new CopyOnWriteArrayList<>();
        ServerInterceptor failure = acknowledgements(new AtomicBoolean(), true, received);
        try (Cluster c = new Cluster(ProtocolMode.MULTI, false, id -> id == 2 ? failure : null)) {
            DidaTradeServerState sender = c.states[0];
            for (int position = 0; position < 16; position++) {
                assertTrue(sender.acceptPhaseTwo(position, 0, 1000 + position).accepted());
                sender.notifyLearners(position, 0, 1000 + position);
            }
            await("failed head was retried", () -> received.size() >= 2);
            assertFalse(pendingEmpty(sender));
            sender.channels[2].shutdownNow();
            await("channel shutdown releases the backlog", () -> pendingEmpty(sender));
            int calls = received.size();
            Thread.sleep(700);
            assertEquals("Closed channel must not retain a retry loop", calls, received.size());
            List<Thread> timers = Thread.getAllStackTraces().keySet().stream()
                    .filter(thread -> thread.getName().equals("learn-delivery-retry")).toList();
            assertEquals("One timer per JVM, not per notification", 1, timers.size());
            assertTrue(timers.get(0).isDaemon());
            System.out.println("LEARN channel shutdown: backlog removed, calls stopped=" + calls + ", one daemon timer");
        }
    }

    static boolean pendingEmpty(DidaTradeServerState state) {
        try {
            Field field = DidaTradeServerState.class.getDeclaredField("pendingLearns");
            field.setAccessible(true);
            Map<?, ?> pending = (Map<?, ?>) field.get(state);
            synchronized (pending) { return pending.isEmpty(); }
        } catch (ReflectiveOperationException error) { throw new AssertionError(error); }
    }

    private static void freeze() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ServerInterceptor count = new ServerInterceptor() {
            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                    ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
                if (call.getMethodDescriptor().getFullMethodName().endsWith("/learn")) calls.incrementAndGet();
                return next.startCall(call, headers);
            }
        };
        try (Cluster c = new Cluster(ProtocolMode.MULTI, false, id -> id == 2 ? count : null)) {
            DidaTradeServerState sender = c.states[0];
            c.states[2].freeze();
            for (int position = 0; position < 17; position++) {
                assertTrue(sender.acceptPhaseTwo(position, 0, 1000 + position).accepted());
                sender.notifyLearners(position, 0, 1000 + position);
            }
            await("one frozen Learn handler entered", () -> calls.get() == 1);
            Thread.sleep(2300); // A new Learn deadline would create cancelled duplicate handlers.
            assertEquals("Freeze retains one pending call instead of retrying blocked handlers", 1, calls.get());
            assertFalse(pendingEmpty(sender));
            c.states[2].unfreeze();
            await("unfreeze acknowledges all queued votes", () -> pendingEmpty(sender));
            assertEquals(17, calls.get());
            assertEquals(0, sender.getCurrentBallot());
            assertEquals(0, c.states[2].getCurrentBallot());
            System.out.println("LEARN freeze: one call during 2300 ms freeze; all 17 votes drained after unfreeze");
        }
    }

    private static ServerInterceptor acknowledgements(AtomicBoolean firstAck, boolean failAll,
                                                       List<DidaTradePaxos.LearnRequest> received) {
        return new ServerInterceptor() {
            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                    ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
                if (!call.getMethodDescriptor().getFullMethodName().endsWith("/learn"))
                    return next.startCall(call, headers);
                boolean loseAck = failAll || firstAck.compareAndSet(true, false);
                ServerCall<Req, Resp> observed = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    @Override public void sendMessage(Resp message) {
                        if (!loseAck) super.sendMessage(message);
                    }
                    @Override public void close(Status status, Metadata trailers) {
                        super.close(loseAck ? Status.UNAVAILABLE.withDescription("Injected lost Learn ACK") : status, trailers);
                    }
                };
                ServerCall.Listener<Req> listener = next.startCall(observed, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                    @Override public void onMessage(Req request) {
                        received.add((DidaTradePaxos.LearnRequest) request);
                        super.onMessage(request); // The production learner processes the vote.
                    }
                };
            }
        };
    }
}
