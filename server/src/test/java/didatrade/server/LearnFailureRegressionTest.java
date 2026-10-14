package didatrade.server;

import didatrade.*;
import io.grpc.*;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static didatrade.server.AuditTestSupport.*;

/** These regressions require recovery; they remain red if dispatched Learn votes are lost. */
public class LearnFailureRegressionTest extends TestCase {
    public void testClassicLearnerRecoversAfterDispatchedLearnTransportFailure() throws Exception { isolated(getClass(), "CLASSIC"); }
    public void testMultiLearnerRecoversAfterDispatchedLearnTransportFailure() throws Exception { isolated(getClass(), "MULTI"); }
    public void testFastLearnerRecoversAfterDispatchedLearnTransportFailure() throws Exception { isolated(getClass(), "FAST"); }

    public static void main(String[] args) {
        try {
            check(ProtocolMode.valueOf(args[0]));
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static void check(ProtocolMode mode) throws Exception {
        int n = mode == ProtocolMode.FAST ? 5 : 3;
        int quorum = mode == ProtocolMode.FAST ? 4 : 2;
        int target = n - 1;
        DispatchedLearnGate gate = new DispatchedLearnGate();
        List<Status> terminalErrors = new CopyOnWriteArrayList<>();
        try (Cluster c = new Cluster(mode, true, id -> id == target ? gate : null)) {
            // Observe actual errors of the production Learn RPCs to this learner.
            ClientInterceptor errors = new ClientInterceptor() {
                @Override public <Req, Resp> ClientCall<Req, Resp> interceptCall(
                        MethodDescriptor<Req, Resp> method, CallOptions options, Channel next) {
                    ClientCall<Req, Resp> call = next.newCall(method, options);
                    if (!method.getFullMethodName().endsWith("/learn")) return call;
                    return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
                        @Override public void start(Listener<Resp> listener, Metadata headers) {
                            super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                                @Override public void onClose(Status status, Metadata trailers) {
                                    super.onClose(status, trailers);
                                    if (!status.isOk()) terminalErrors.add(status);
                                }
                            }, headers);
                        }
                    };
                }
            };
            for (DidaTradeServerState state : c.states)
                state.async_stubs[target] = state.async_stubs[target].withInterceptors(errors);

            // Supply the target's payload first, so its stall cannot be blamed on
            // missing client data. Then submit one identical request everywhere.
            Future<DidaTradeMain.AddUserReply> original = c.pool.submit(() -> c.add(target, 101, 40));
            await("target has original client payload", () -> c.states[target].req_history.getIfPending(101) != null);
            List<Future<DidaTradeMain.AddUserReply>> healthyReplies = new ArrayList<>();
            for (int id = 0; id < target; id++) {
                final int replica = id;
                healthyReplies.add(c.pool.submit(() -> c.add(replica, 101, 40)));
            }
            for (Future<DidaTradeMain.AddUserReply> reply : healthyReplies)
                assertTrue(reply.get(8, TimeUnit.SECONDS).getResult());
            // Fast may suppress a fifth local vote after other learners have already
            // decided. A real decision quorum, rather than all five votes, is required.
            await("decision quorum dispatched Learn to target", () -> gate.requests.stream()
                    .map(DidaTradePaxos.LearnRequest::getServerid).distinct().count() >= quorum);
            int dispatched = gate.requests.size();
            assertFalse(c.states[target].paxos_log.isDecided(0));
            assertEquals(0L, c.states[target].getAppliedVersion());
            assertTrue(c.states[0].paxos_log.isDecided(0));

            // Actual HTTP/2 transport termination AFTER request delivery. State and
            // coordinator stay alive; this is neither pre-dispatch failure nor restart.
            c.servers[target].shutdownNow();
            await("production Learn calls terminated with transport errors", () -> terminalErrors.size() >= dispatched);
            assertTrue(terminalErrors.toString(), terminalErrors.stream()
                    .allMatch(status -> status.getCode() == Status.Code.UNAVAILABLE
                            || status.getCode() == Status.Code.CANCELLED));
            gate.hold = false;
            c.startTransport(target);
            DidaTradePaxos.PhaseOneReply probe = c.paxos(target).phaseone(DidaTradePaxos.PhaseOneRequest
                    .newBuilder().setInstance(0).setRequestballot(0).build());
            assertTrue(probe.getAccepted());
            assertEquals(0, c.states[target].getCurrentBallot());
            c.read(target, 201, 0); // A fresh main-service RPC also succeeds on restored transport.

            Future<?> retransmission = c.pool.submit(() -> c.main(target).withDeadlineAfter(900, TimeUnit.MILLISECONDS)
                    .adduser(DidaTradeMain.AddUserRequest.newBuilder().setReqid(101)
                            .setUserid(42).setCash(40).setStock(20).build()));
            boolean recovered = eventually(2000, () -> c.states[target].getAppliedVersion() == 1);
            boolean retransmissionTimedOut = false;
            try { retransmission.get(2, TimeUnit.SECONDS); }
            catch (ExecutionException error) {
                assertTrue(error.getCause() instanceof StatusRuntimeException);
                assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusRuntimeException) error.getCause()).getStatus().getCode());
                retransmissionTimedOut = true;
            }
            System.out.println("AUD-03 mode=" + mode + " dispatched=" + gate.requests.size()
                    + " terminalLearnErrors=" + terminalErrors.size() + " errors=" + terminalErrors
                    + " recoveredWithoutReplay=" + recovered + " retransmissionTimedOut=" + retransmissionTimedOut
                    + " originalRpcFinished=" + original.isDone());

            // Replay the real captured votes for cleanup and as a control proving
            // that restored connectivity + the missing certificate unblock execution.
            for (DidaTradePaxos.LearnRequest vote : gate.requests) c.paxos(target).learn(vote);
            await("explicit replay executes original payload", () -> c.states[target].getAppliedVersion() == 1);
            assertEquals(40, c.read(target, 301, 1).getBalance());
            System.out.println("AUD-03 explicitReplayRestoredExecution=true");
            assertTrue("AUD-03 " + mode + ": dispatched Learn RPC errors permanently strand the learner;"
                    + " transport and client RPCs recovered, but no vote replay occurred", recovered);
        }
    }

    /** Holds only messages already delivered to the server; never manufactures an error status. */
    private static final class DispatchedLearnGate implements ServerInterceptor {
        volatile boolean hold = true;
        final List<DidaTradePaxos.LearnRequest> requests = new CopyOnWriteArrayList<>();

        @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
            ServerCall.Listener<Req> listener = next.startCall(call, headers);
            if (!hold || !call.getMethodDescriptor().getFullMethodName().endsWith("/learn")) return listener;
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                @Override public void onMessage(Req message) {
                    requests.add((DidaTradePaxos.LearnRequest) message);
                    // Withhold processing until the transport is forcibly closed.
                }
                @Override public void onHalfClose() { }
            };
        }
    }
}
