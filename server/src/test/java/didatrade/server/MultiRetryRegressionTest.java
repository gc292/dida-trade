package didatrade.server;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import io.grpc.*;
import junit.framework.TestCase;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static didatrade.server.AuditTestSupport.*;

/** Requires an independently learned decision to invalidate its expired Phase 2 retry. */
public class MultiRetryRegressionTest extends TestCase {
    public void testLearnedDecisionAndFailedPhaseTwoReturnCoordinatorToWaiting() throws Exception {
        isolated(getClass(), "learn-before-timeout");
    }

    public void testDecisionAfterRetrySchedulingClearsRetryAndReturnsToWaiting() throws Exception {
        isolated(getClass(), "learn-after-scheduling");
    }

    public static void main(String[] args) {
        try {
            if (args[0].equals("learn-before-timeout")) check();
            else if (args[0].equals("learn-after-scheduling")) decisionAfterScheduling();
            else throw new IllegalArgumentException(args[0]);
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static void check() throws Exception {
        AtomicInteger suppressedReplies = new AtomicInteger();
        ServerInterceptor replies = new ServerInterceptor() {
            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                    ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
                if (!call.getMethodDescriptor().getFullMethodName().endsWith("/phasetwo"))
                    return next.startCall(call, headers);
                return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    @Override public void sendMessage(Resp reply) {
                        assertTrue(((DidaTradePaxos.PhaseTwoReply) reply).getAccepted());
                        suppressedReplies.incrementAndGet();
                        // The production handler continues to send Learn normally.
                    }
                    @Override public void close(Status status, Metadata trailers) { }
                }, headers);
            }
        };
        try (Cluster c = new Cluster(ProtocolMode.MULTI, true, ignored -> replies)) {
            List<Status> failures = new CopyOnWriteArrayList<>();
            ClientInterceptor deadlines = new ClientInterceptor() {
                @Override public <Req, Resp> ClientCall<Req, Resp> interceptCall(
                        MethodDescriptor<Req, Resp> method, CallOptions options, Channel next) {
                    ClientCall<Req, Resp> call = next.newCall(method, options);
                    if (!method.getFullMethodName().endsWith("/phasetwo")) return call;
                    return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
                        @Override public void start(Listener<Resp> listener, Metadata headers) {
                            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                                @Override public void onClose(Status status, Metadata trailers) {
                                    super.onClose(status, trailers);
                                    if (!status.isOk()) failures.add(status);
                                }
                            }, headers);
                        }
                    };
                }
            };
            for (int id = 0; id < c.states.length; id++)
                c.states[0].async_stubs[id] = c.states[0].async_stubs[id].withInterceptors(deadlines);
            // Learn decides before the suppressed Phase 2 replies hit their deadlines.
            DidaTradeMain.AddUserReply result = c.add(0, 101, 40);
            assertTrue(result.getResult());
            await("all Phase 2 accepts happened", () -> suppressedReplies.get() == 3);
            assertTrue(c.states[0].paxos_log.isDecided(0));
            assertEquals(1L, c.states[0].getAppliedVersion());
            assertEquals(0, c.states[0].getCurrentBallot());
            Object loop = c.states[0].main_loop;
            Method ready = loop.getClass().getDeclaredMethod("retryReady");
            ready.setAccessible(true);
            Field retries = loop.getClass().getDeclaredField("retryNotBefore");
            retries.setAccessible(true);
            // Actual outbound RPC deadlines (2 s), followed by the 500 ms retry delay.
            boolean staleReady = eventually(4000, () -> invokeReady(ready, loop));
            await("all local Phase 2 RPCs failed at their deadlines", () -> failures.size() == 3);
            assertTrue(failures.toString(), failures.stream()
                    .allMatch(status -> status.getCode() == Status.Code.DEADLINE_EXCEEDED));
            Thread worker = c.states[0].main_loop_worker;
            var threads = ManagementFactory.getThreadMXBean();
            if (threads.isThreadCpuTimeSupported()) threads.setThreadCpuTimeEnabled(true);
            long before = threads.isThreadCpuTimeSupported() ? threads.getThreadCpuTime(worker.threadId()) : -1;
            Thread.sleep(150);
            long after = threads.isThreadCpuTimeSupported() ? threads.getThreadCpuTime(worker.threadId()) : -1;
            long cpuNanos = before >= 0 && after >= 0 ? after - before : -1;
            Thread.State sampledState = worker.getState(); // Sample before holding its monitor.
            synchronized (loop) {
                Map<?, ?> retryState = (Map<?, ?>) retries.get(loop);
                System.out.println("AUD-04 staleRetryReady=" + staleReady + " retryKeys=" + retryState.keySet()
                        + " workerState=" + sampledState + " cpuNanosOver150ms=" + cpuNanos
                        + " PhaseTwoSends=" + suppressedReplies.get() + " terminalPhaseTwoErrors=" + failures
                        + " appliedVersion=" + result.getAppliedVersion());
            }
            assertEquals("No extra Phase 2 sends despite CPU spin", 3, suppressedReplies.get());
            assertEquals("The decided write executes exactly once", 1L, c.states[0].getAppliedVersion());
            assertFalse("AUD-04: an already decided instance still has a runnable retry after its deadline", staleReady);
            await("idle coordinator waiting", () -> worker.getState() == Thread.State.WAITING
                    || worker.getState() == Thread.State.TIMED_WAITING);
        }
    }

    private static boolean invokeReady(Method method, Object loop) {
        try {
            synchronized (loop) { return (boolean) method.invoke(loop); }
        } catch (ReflectiveOperationException error) { throw new AssertionError(error); }
    }

    private static void decisionAfterScheduling() throws Exception {
        var hold = new java.util.concurrent.atomic.AtomicBoolean(true);
        List<DidaTradePaxos.LearnRequest> leaderVotes = new CopyOnWriteArrayList<>();
        try (Cluster c = new Cluster(ProtocolMode.MULTI, true, id -> new ServerInterceptor() {
            @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                    ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
                String method = call.getMethodDescriptor().getFullMethodName();
                if (method.endsWith("/phasetwo")) {
                    return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                        @Override public void sendMessage(Resp message) { }
                        @Override public void close(Status status, Metadata trailers) { }
                    }, headers);
                }
                ServerCall.Listener<Req> listener = next.startCall(call, headers);
                if (!method.endsWith("/learn") || !hold.get()) return listener;
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                    @Override public void onMessage(Req request) {
                        if (id == 0) leaderVotes.add((DidaTradePaxos.LearnRequest) request);
                    }
                    @Override public void onHalfClose() { }
                };
            }
        })) {
            var write = c.pool.submit(() -> c.add(0, 101, 40));
            Object loop = c.states[0].main_loop;
            Field retries = loop.getClass().getDeclaredField("retryNotBefore");
            retries.setAccessible(true);
            await("timeout scheduled a still-undecided retry", () -> retryCount(retries, loop) == 1);
            assertFalse(write.isDone());
            assertEquals(0L, c.states[0].getAppliedVersion());
            assertTrue(leaderVotes.stream().map(DidaTradePaxos.LearnRequest::getServerid).distinct().count() >= 2);
            hold.set(false);
            for (DidaTradePaxos.LearnRequest vote : leaderVotes) c.paxos(0).learn(vote);
            assertTrue(write.get(3, TimeUnit.SECONDS).getResult());
            await("Learn removes the scheduled retry", () -> retryCount(retries, loop) == 0);
            Thread.sleep(600); // Also pass the now-obsolete retry's original deadline.
            Thread worker = c.states[0].main_loop_worker;
            await("worker idle after later Learn", () -> worker.getState() == Thread.State.WAITING);
            assertEquals(0, retryCount(retries, loop));
            assertEquals(1L, c.states[0].getAppliedVersion());
            System.out.println("AUD-04 later Learn: retry removed, worker WAITING, executed once");
        }
    }

    private static int retryCount(Field field, Object loop) {
        try {
            synchronized (loop) { return ((Map<?, ?>) field.get(loop)).size(); }
        } catch (ReflectiveOperationException error) { throw new AssertionError(error); }
    }
}
