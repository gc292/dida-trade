package didatrade.server;

import didatrade.configs.ConfigurationScheduler;
import junit.framework.TestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FastAcceptorTest extends TestCase {
    public void testOnlyPerPositionRecoveryReleasesHistoricalReservation() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 0));
        assertTrue(log.acceptFastProposal(0, 0, 101));
        assertTrue(log.getReservedCommandIds(1).contains(101));
        assertFalse(log.isReservationSuperseded(0, 0, 101));
        assertTrue(log.authorizeFastInstance(1, 1));
        assertTrue(log.getReservedCommandIds(1).contains(101));
        assertTrue(log.authorizeFastInstance(0, 1));
        assertFalse(log.getReservedCommandIds(1).contains(101));
        assertTrue(log.isReservationSuperseded(0, 0, 101));
        assertSnapshot(log, 0, 0, 101);
        assertTrue(log.acceptFastProposal(0, 1, 101));
        assertTrue(log.getReservedCommandIds(1).contains(101));
        assertTrue(log.accept(0, 2, 201));
        assertFalse(log.getReservedCommandIds(2).contains(101));
        assertTrue(log.getReservedCommandIds(2).contains(201));
    }

    public void testNextFastPositionWaitsForEarlierAuthorizationAndRespectsLowerBound() {
        PaxosLog log = new PaxosLog();
        assertNull(log.getNextFastInstance(1, 0));
        assertTrue(log.authorizeFastInstance(7, 1));
        assertTrue(log.authorizeFastInstance(2, 1));
        assertTrue(log.authorizeFastInstance(5, 1));
        assertNull(log.getNextFastInstance(1, 0));
        assertEquals(2, log.getNextFastInstance(1, 2).intValue());
        assertNull(log.getNextFastInstance(1, 3));
        assertEquals(5, log.getNextFastInstance(1, 5).intValue());
        assertNull(log.getNextFastInstance(1, 8));
        assertNull(log.getNextFastInstance(2, 2));
        assertTrue(log.acceptFastProposal(2, 1, 10));
        log.decideEntry(3, 1, 30);
        log.decideEntry(4, 1, 40);
        assertEquals(5, log.getNextFastInstance(1, 2).intValue());
        log.decideEntry(5, 1, 20);
        log.decideEntry(6, 1, 60);
        assertEquals(7, log.getNextFastInstance(1, 2).intValue());
        assertTrue(log.acceptFastProposal(7, 1, 70));
        assertNull(log.getNextFastInstance(1, 2));
    }

    public void testHigherVoteInvalidatesOldSlotButNewAuthorizationCanUsePreviousVoteSlot() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 1));
        assertTrue(log.accept(0, 2, 10));
        assertNull(log.getNextFastInstance(1, 0));
        assertTrue(log.authorizeFastInstance(0, 3));
        assertEquals(0, log.getNextFastInstance(3, 0).intValue());
        assertTrue(log.acceptFastProposal(0, 3, 20));
        assertNull(log.getNextFastInstance(3, 0));
    }

    public void testAnyIsAuthorizationAndDoesNotAppearInSnapshot() {
        PaxosLog log = new PaxosLog();
        assertFalse(log.acceptFastProposal(0, 1, 10));
        assertTrue(log.authorizeFastInstance(0, 1));
        assertTrue(log.authorizeFastInstance(0, 1));
        assertTrue(log.getAcceptedEntries().isEmpty());
        assertFalse(log.isDecided(0));
        assertFalse(log.acceptFastProposal(0, 0, 10));
        assertFalse(log.acceptFastProposal(0, 2, 10));
        assertTrue(log.acceptFastProposal(0, 1, 10));
    }

    public void testRepeatedAnyPreservesTheFirstVote() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 1));
        assertTrue(log.acceptFastProposal(0, 1, 10));
        assertTrue(log.authorizeFastInstance(0, 1));
        assertTrue(log.acceptFastProposal(0, 1, 10));
        assertFalse(log.acceptFastProposal(0, 1, 20));
        assertSnapshot(log, 0, 1, 10);
    }

    public void testAnyCannotReopenConcreteRecoveryInTheSameBallot() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 1));
        assertTrue(log.acceptFastProposal(0, 1, 10));
        assertTrue(log.accept(0, 2, 20));
        assertFalse(log.authorizeFastInstance(0, 2));
        assertFalse(log.authorizeFastInstance(0, 1));
        assertFalse(log.acceptFastProposal(0, 2, 30));
        assertSnapshot(log, 0, 2, 20);
    }

    public void testNewAuthorizationPreservesOldConcreteSnapshotUntilNewVote() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 1));
        assertTrue(log.acceptFastProposal(0, 1, 10));
        assertTrue(log.authorizeFastInstance(0, 2));
        assertFalse(log.authorizeFastInstance(0, 1));
        assertFalse(log.acceptFastProposal(0, 1, 10));
        assertSnapshot(log, 0, 1, 10);
        assertTrue(log.acceptFastProposal(0, 2, 20));
        assertSnapshot(log, 0, 2, 20);
    }

    public void testAuthorizationIsSpecificToPositionAndDoesNotReopenDecision() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 1));
        assertFalse(log.acceptFastProposal(1, 1, 10));
        log.decideEntry(0, 1, 10);
        assertFalse(log.authorizeFastInstance(0, 1));
        assertFalse(log.authorizeFastInstance(0, 2));
        assertFalse(log.acceptFastProposal(0, 1, 20));
        assertEquals(10, log.getDecidedCommandId(0).intValue());
    }

    public void testDecisionBeforeLocalVoteRejectsEvenTheMatchingValue() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 1));
        log.decideEntry(0, 1, 10);
        assertFalse(log.acceptFastProposal(0, 1, 10));
        assertTrue(log.getAcceptedEntries().isEmpty());
        assertEquals(10, log.getDecidedCommandId(0).intValue());
    }

    public void testConcurrentDifferentProposalsProduceExactlyOneVote() throws Exception {
        PaxosLog log = new PaxosLog();
        assertTrue(log.authorizeFastInstance(0, 1));
        var pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var first = pool.submit(() -> { start.await(); return log.acceptFastProposal(0, 1, 10); });
            var second = pool.submit(() -> { start.await(); return log.acceptFastProposal(0, 1, 20); });
            start.countDown();
            boolean acceptedFirst = first.get(3, TimeUnit.SECONDS);
            boolean acceptedSecond = second.get(3, TimeUnit.SECONDS);
            assertTrue(acceptedFirst != acceptedSecond);
            assertSnapshot(log, 0, 1, acceptedFirst ? 10 : 20);
        } finally {
            pool.shutdownNow();
        }
    }

    public void testStateQuorumsAndPromisesInIsolatedProcess() throws Exception {
        runIsolated("promises");
    }

    public void testPrepareSnapshotAndDirectVoteAreAtomicInIsolatedProcess() throws Exception {
        runIsolated("race");
    }

    public void testFastTransportUsesFourRepliesAndConventionalPhaseTwoUsesThree() throws Exception {
        runIsolated("transport");
    }

    public void testFastLearnWaitsForARecoveringChannelInsteadOfDroppingTheVote() throws Exception {
        runIsolated("late-learner");
    }

    private static void assertSnapshot(PaxosLog log, int position, int ballot, int value) {
        var entries = log.getAcceptedEntries();
        assertEquals(1, entries.size());
        assertEquals(position, entries.get(0).getInstance());
        assertEquals(ballot, entries.get(0).getValballot());
        assertEquals(value, entries.get(0).getValue());
    }

    private void runIsolated(String scenario) throws Exception {
        Path directory = Path.of("target", "fast-acceptor-regression");
        Files.createDirectories(directory);
        Path output = directory.resolve(scenario + ".log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, FastAcceptorTest.class.getName(), scenario)
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue("Timed out: " + output, process.waitFor(20, TimeUnit.SECONDS));
            assertEquals(Files.readString(output), 0, process.exitValue());
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    // Isolate gRPC channels and explicitly controlled component lifecycles from Surefire.
    public static void main(String[] args) {
        try {
            ProtocolMode mode = args[0].equals("late-learner") ? ProtocolMode.FAST : ProtocolMode.CLASSIC;
            DidaTradeServerState state = new DidaTradeServerState(19000, 0, 'B', mode, false);
            if (args[0].equals("promises")) checkPromisesAndQuorums(state);
            else if (args[0].equals("race")) checkPrepareRace(state);
            else if (args[0].equals("transport")) checkTransport(state);
            else if (args[0].equals("late-learner")) checkLateLearner(state);
            else throw new IllegalArgumentException("Unknown scenario");
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static void checkPromisesAndQuorums(DidaTradeServerState state) {
        assertEquals(4, state.getFastQuorum(0));
        assertEquals(3, state.scheduler.quorum(0).intValue());
        assertEquals(4, state.scheduler.recoveryQuorum(0).intValue());
        assertTrue(state.authorizeFastInstance(0, 1).accepted());
        assertTrue(state.acceptFastProposal(0, 1, 10).accepted());
        var future = state.acceptFastProposal(1, 2, 20);
        assertFalse(future.accepted());
        assertEquals(1, future.maxBallot());
        var prepared = state.prepareAcceptor(2);
        assertTrue(prepared.accepted());
        assertEquals(10, prepared.acceptedEntries().get(0).getValue());
        assertEquals(1, prepared.acceptedEntries().get(0).getValballot());
        assertFalse(state.acceptFastProposal(0, 1, 10).accepted());
        assertFalse(state.authorizeFastInstance(0, 1).accepted());
        assertEquals(2, state.getCurrentBallot());
        assertFalse(state.acceptFastProposal(1, 2, 20).accepted());
        assertTrue(state.authorizeFastInstance(1, 2).accepted());
        assertTrue(state.acceptFastProposal(1, 2, 20).accepted());

        // Different ballot configurations prove the helper uses its argument.
        state.scheduler = new ConfigurationScheduler('B') {
            @Override public Integer superMajority(int ballot) {
                int n = acceptors(ballot).size();
                return n - (n - 1) / 3;
            }
            @Override public List<Integer> acceptors(int ballot) {
                return ballot == 0 ? List.of(0, 1, 2) : List.of(0, 1, 2, 3, 4);
            }
        };
        assertEquals(3, state.getFastQuorum(0));
        assertEquals(4, state.getFastQuorum(1));
        state.scheduler.setSchedule('A');
        assertEquals(2, state.scheduler.quorum(1).intValue());
    }

    private static void checkPrepareRace(DidaTradeServerState state) throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        try {
            for (int position = 0; position < 50; position++) {
                int slot = position;
                int ballot = position * 2 + 1;
                int value = 100 + position;
                assertTrue(state.authorizeFastInstance(slot, ballot).accepted());
                CountDownLatch start = new CountDownLatch(1);
                var vote = pool.submit(() -> {
                    start.await();
                    return state.acceptFastProposal(slot, ballot, value);
                });
                var prepare = pool.submit(() -> {
                    start.await();
                    return state.prepareAcceptor(ballot + 1);
                });
                start.countDown();
                var voteResult = vote.get(3, TimeUnit.SECONDS);
                var snapshot = prepare.get(3, TimeUnit.SECONDS);
                assertTrue(snapshot.accepted());
                var entry = snapshot.acceptedEntries().stream()
                        .filter(accepted -> accepted.getInstance() == slot).findFirst();
                assertEquals(voteResult.accepted(), entry.isPresent());
                if (entry.isPresent()) {
                    assertEquals(ballot, entry.get().getValballot());
                    assertEquals(value, entry.get().getValue());
                }
                assertFalse(state.acceptFastProposal(slot, ballot, value).accepted());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static void checkLateLearner(DidaTradeServerState state) throws Exception {
        int port;
        try (java.net.ServerSocket reserved = new java.net.ServerSocket(0)) { port = reserved.getLocalPort(); }
        state.channels[0].shutdownNow();
        state.channels[0] = io.grpc.ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        state.async_stubs[0] = didatrade.DidaTradePaxosServiceGrpc.newStub(state.channels[0]);
        io.grpc.Server server = null;
        try {
            state.channels[0].getState(true);
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (state.channels[0].getState(false) != io.grpc.ConnectivityState.TRANSIENT_FAILURE) {
                assertTrue("Channel must first observe an unavailable peer", System.nanoTime() < end);
                Thread.sleep(10);
            }
            state.notifyLearners(0, 0, 101);
            TransportAcceptor peer = new TransportAcceptor(0);
            server = io.grpc.ServerBuilder.forPort(port).addService(peer).build().start();
            assertTrue("The queued vote must reach the learner after its channel recovers",
                    peer.learnReceived.await(5, TimeUnit.SECONDS));
            assertEquals(101, peer.learnRequest.getValue());
        } finally {
            for (var channel : state.channels) channel.shutdownNow();
            if (server != null) server.shutdownNow();
        }
    }

    private static void checkTransport(DidaTradeServerState state) throws Exception {
        var peers = new ArrayList<TransportAcceptor>();
        var servers = new ArrayList<io.grpc.Server>();
        try {
            for (int id = 0; id < 5; id++) {
                TransportAcceptor peer = new TransportAcceptor(id);
                peers.add(peer);
                io.grpc.Server server = io.grpc.ServerBuilder.forPort(0).addService(peer).build().start();
                servers.add(server);
                state.channels[id].shutdownNow();
                state.channels[id] = io.grpc.ManagedChannelBuilder.forAddress("localhost", server.getPort())
                        .usePlaintext().build();
                state.async_stubs[id] = didatrade.DidaTradePaxosServiceGrpc.newStub(state.channels[id]);
            }

            var preparation = state.runFastPhaseOne(10);
            assertTrue(preparation.getAccepted());
            assertEquals(10, preparation.getMaxballot());
            assertEquals(20, preparation.getRecoveredEntries().get(0).getValue());
            for (TransportAcceptor peer : peers) {
                assertTrue(peer.phaseOneReceived.await(5, TimeUnit.SECONDS));
                assertEquals(10, peer.phaseOneRequest.getRequestballot());
            }

            var conventional = state.startPhaseTwo(1, 10, 30);
            assertTrue(conventional.waitUntilDone().getAccepted());
            for (TransportAcceptor peer : peers) {
                assertTrue(peer.phaseTwoReceived.await(5, TimeUnit.SECONDS));
                peer.phaseTwoReceived = new CountDownLatch(1);
                peer.hold = true;
            }

            var fast = state.startFastPhaseTwo(2, 10, 40);
            CountDownLatch completed = new CountDownLatch(1);
            fast.onCompletion(completed::countDown);
            for (TransportAcceptor peer : peers) {
                assertTrue(peer.phaseTwoReceived.await(5, TimeUnit.SECONDS));
                assertEquals(2, peer.phaseTwoRequest.getInstance());
                assertEquals(10, peer.phaseTwoRequest.getRequestballot());
                assertEquals(40, peer.phaseTwoRequest.getValue());
                assertFalse(peer.phaseTwoRequest.getAny());
            }
            // Wait for processing of the three replies: they must not complete Fast.
            assertFalse(completed.await(150, TimeUnit.MILLISECONDS));
            peers.get(3).replyPhaseTwo(true, 10);
            assertTrue(completed.await(5, TimeUnit.SECONDS));
            assertTrue(fast.getResult().getAccepted());

            for (TransportAcceptor peer : peers) peer.phaseTwoReceived = new CountDownLatch(1);
            var authorization = state.startFastAny(3, 10);
            CountDownLatch authorized = new CountDownLatch(1);
            authorization.onCompletion(authorized::countDown);
            for (TransportAcceptor peer : peers) {
                assertTrue(peer.phaseTwoReceived.await(5, TimeUnit.SECONDS));
                assertTrue(peer.phaseTwoRequest.getAny());
                assertEquals(3, peer.phaseTwoRequest.getInstance());
                assertEquals(10, peer.phaseTwoRequest.getRequestballot());
                assertEquals(0, peer.phaseTwoRequest.getValue());
            }
            assertFalse(authorized.await(150, TimeUnit.MILLISECONDS));
            peers.get(3).replyPhaseTwo(true, 10);
            assertTrue(authorized.await(5, TimeUnit.SECONDS));
            assertTrue(authorization.getResult().getAccepted());
            assertFalse(state.paxos_log.isDecided(3));
            assertTrue(state.paxos_log.getAcceptedEntries().isEmpty());

            // A cancelled originating RPC must not cancel outbound Learn notifications.
            io.grpc.Context.CancellableContext originatingRpc = io.grpc.Context.current().withCancellation();
            originatingRpc.cancel(new IllegalStateException("Original RPC has ended"));
            try {
                originatingRpc.run(() -> state.notifyLearners(7, 10, 50));
                for (TransportAcceptor peer : peers) {
                    assertTrue(peer.learnReceived.await(5, TimeUnit.SECONDS));
                    assertEquals(7, peer.learnRequest.getInstance());
                    assertEquals(10, peer.learnRequest.getBallot());
                    assertEquals(50, peer.learnRequest.getValue());
                    assertEquals(0, peer.learnRequest.getServerid());
                }
            } finally {
                originatingRpc.close();
            }
        } finally {
            for (var channel : state.channels) channel.shutdownNow();
            for (var server : servers) server.shutdownNow();
        }
    }

    private static final class TransportAcceptor
            extends didatrade.DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
        private final int id;
        private final CountDownLatch phaseOneReceived = new CountDownLatch(1);
        private volatile CountDownLatch phaseTwoReceived = new CountDownLatch(1);
        private volatile boolean hold;
        private volatile didatrade.DidaTradePaxos.PhaseOneRequest phaseOneRequest;
        private volatile didatrade.DidaTradePaxos.PhaseTwoRequest phaseTwoRequest;
        private volatile io.grpc.stub.StreamObserver<didatrade.DidaTradePaxos.PhaseTwoReply> observer;
        private final CountDownLatch learnReceived = new CountDownLatch(1);
        private volatile didatrade.DidaTradePaxos.LearnRequest learnRequest;

        private TransportAcceptor(int id) { this.id = id; }

        @Override
        public void learn(didatrade.DidaTradePaxos.LearnRequest request,
                          io.grpc.stub.StreamObserver<didatrade.DidaTradePaxos.LearnReply> response) {
            learnRequest = request;
            learnReceived.countDown();
            response.onNext(didatrade.DidaTradePaxos.LearnReply.newBuilder()
                    .setInstance(request.getInstance()).setBallot(request.getBallot()).build());
            response.onCompleted();
        }

        @Override
        public void phaseone(didatrade.DidaTradePaxos.PhaseOneRequest request,
                             io.grpc.stub.StreamObserver<didatrade.DidaTradePaxos.PhaseOneReply> response) {
            phaseOneRequest = request;
            phaseOneReceived.countDown();
            if (id == 4) {
                response.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
                return;
            }
            response.onNext(didatrade.DidaTradePaxos.PhaseOneReply.newBuilder().setServerid(id)
                    .setRequestballot(request.getRequestballot()).setAccepted(true).setMaxballot(10)
                    .addAcceptedEntries(didatrade.DidaTradePaxos.AcceptedEntry.newBuilder()
                            .setInstance(0).setValue(20).setValballot(7)).build());
            response.onCompleted();
        }

        @Override
        public void phasetwo(didatrade.DidaTradePaxos.PhaseTwoRequest request,
                             io.grpc.stub.StreamObserver<didatrade.DidaTradePaxos.PhaseTwoReply> response) {
            phaseTwoRequest = request;
            observer = response;
            if (id < 3) replyPhaseTwo(true, request.getRequestballot());
            else if (!hold) response.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
            phaseTwoReceived.countDown();
        }

        private void replyPhaseTwo(boolean accepted, int maxBallot) {
            observer.onNext(didatrade.DidaTradePaxos.PhaseTwoReply.newBuilder().setServerid(id)
                    .setInstance(phaseTwoRequest.getInstance()).setRequestballot(phaseTwoRequest.getRequestballot())
                    .setAccepted(accepted).setMaxballot(maxBallot).build());
            observer.onCompleted();
        }
    }
}
