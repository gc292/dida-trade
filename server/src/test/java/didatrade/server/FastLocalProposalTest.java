package didatrade.server;

import didatrade.DidaTradePaxos;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import junit.framework.TestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FastLocalProposalTest extends TestCase {
    public void testRequestBeforeAnyAndRepeatedAuthorization() throws Exception { runIsolated("before"); }
    public void testRequestAfterAnyAndLocalDuplicateExclusion() throws Exception { runIsolated("after"); }
    public void testLosingRequestRemainsEligibleAfterDecision() throws Exception { runIsolated("loser"); }
    public void testHigherPromiseRejectsOldAuthorizationAndLocalVote() throws Exception { runIsolated("promise"); }
    public void testConventionalModesRejectAnyAndKeepConcreteNotifications() throws Exception { runIsolated("modes"); }
    public void testFiveAcceptorsAgreeWithoutCollision() throws Exception { runIsolated("agreement"); }
    public void testIndependentLocalOrdersProduceRealFastCollision() throws Exception { runIsolated("collision"); }
    public void testSameRequestCanReachDifferentPositionsOnDifferentReplicas() throws Exception { runIsolated("positions"); }
    public void testHigherAnyReleasesHistoricalVoteWithoutErasingAcceptedSnapshot() throws Exception { runIsolated("reopen"); }

    private void runIsolated(String scenario) throws Exception {
        Path output = Path.of("target", "fast-local-proposal-" + scenario + ".log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, FastLocalProposalTest.class.getName(), scenario)
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

    public static void main(String[] args) {
        try {
            switch (args[0]) {
                case "before" -> checkBefore();
                case "after" -> checkAfter();
                case "loser" -> checkLoser();
                case "promise" -> checkPromise();
                case "modes" -> checkModes();
                case "agreement" -> checkVotes(false);
                case "collision" -> checkVotes(true);
                case "positions" -> checkPositions();
                case "reopen" -> checkReopen();
                default -> throw new IllegalArgumentException("Unknown scenario");
            }
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static RequestRecord pending(LocalState state, int id) {
        return state.req_history.addToPending(id,
                new RequestRecord(id, new DidaTradeCommand(DidaTradeAction.BUY, 42, 10)));
    }

    private static RecordingObserver any(LocalState state, int position, int ballot) {
        RecordingObserver observer = new RecordingObserver();
        new DidaTradePaxosServiceImpl(state).phasetwo(DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(position).setRequestballot(ballot).setAny(true).setValue(999).build(), observer);
        return observer;
    }

    private static void assertAuthorized(RecordingObserver response) {
        assertNull(response.error);
        assertTrue(response.completed);
        assertTrue(response.reply.getAccepted());
    }

    private static void checkBefore() {
        LocalState state = new LocalState(1, ProtocolMode.FAST);
        RequestRecord request = pending(state, 101);
        state.loop.proposePendingRequests(0);
        assertTrue(state.notifications.isEmpty());
        assertAuthorized(any(state, 0, 0));
        assertTrue(state.loop.takeWork());
        assertTrue(state.paxos_log.getAcceptedEntries().isEmpty());
        assertTrue(state.notifications.isEmpty());
        state.loop.proposePendingRequests(0); // A follower must vote too.
        assertEquals(List.of(new Vote(0, 0, 101, 1)), state.notifications);
        assertSame(request, state.req_history.getIfPending(101));
        assertFalse(state.paxos_log.isDecided(0));
        assertAuthorized(any(state, 0, 0));
        state.loop.proposePendingRequests(0);
        assertEquals(1, state.notifications.size());
        assertEquals(101, state.paxos_log.getAcceptedEntries().get(0).getValue());
    }

    private static void checkAfter() {
        LocalState state = new LocalState(1, ProtocolMode.FAST);
        assertAuthorized(any(state, 2, 0));
        assertAuthorized(any(state, 0, 0));
        state.loop.proposePendingRequests(0);
        assertTrue(state.notifications.isEmpty());
        pending(state, 101);
        state.loop.proposePendingRequests(0);
        assertEquals(List.of(new Vote(0, 0, 101, 1)), state.notifications);
        assertNull(state.paxos_log.getNextFastInstance(0, 0));
        pending(state, 102);
        state.loop.proposePendingRequests(0);
        assertEquals(1, state.notifications.size()); // Slot 1 has not received ANY yet.
        assertAuthorized(any(state, 1, 0));
        state.loop.proposePendingRequests(0);
        assertEquals(new Vote(1, 0, 102, 1), state.notifications.get(1));
        pending(state, 103);
        state.loop.proposePendingRequests(0);
        assertEquals(new Vote(2, 0, 103, 1), state.notifications.get(2));
        state.loop.proposePendingRequests(0);
        assertEquals(3, state.notifications.size());
        assertNotNull(state.req_history.getIfPending(101));
        assertNotNull(state.req_history.getIfPending(102));
    }

    private static void checkLoser() {
        LocalState state = new LocalState(1, ProtocolMode.FAST);
        RequestRecord loser = pending(state, 101);
        assertAuthorized(any(state, 0, 0));
        state.loop.proposePendingRequests(0);
        pending(state, 102);
        state.prepareAcceptor(1);
        state.paxos_log.decideEntry(0, 1, 102);
        assertAuthorized(any(state, 1, 1));
        state.loop.proposePendingRequests(1);
        assertEquals(new Vote(1, 1, 101, 1), state.notifications.get(1));
        assertSame(loser, state.req_history.getIfPending(101));
        assertNull(state.req_history.getIfProcessed(101));
    }

    private static void checkPromise() {
        LocalState state = new LocalState(1, ProtocolMode.FAST);
        pending(state, 101);
        assertAuthorized(any(state, 0, 0));
        state.loop.takeWork();
        state.prepareAcceptor(1);
        state.loop.proposePendingRequests(0);
        assertTrue(state.notifications.isEmpty());
        RecordingObserver rejected = any(state, 0, 0);
        assertFalse(rejected.reply.getAccepted());
        assertEquals(1, rejected.reply.getMaxballot());
        assertFalse(state.loop.takeWork());
        assertTrue(state.paxos_log.getAcceptedEntries().isEmpty());
        assertNotNull(state.req_history.getIfPending(101));
    }

    private static void checkModes() {
        for (ProtocolMode mode : new ProtocolMode[] {ProtocolMode.CLASSIC, ProtocolMode.MULTI}) {
            LocalState state = new LocalState(0, mode);
            RecordingObserver rejected = any(state, 0, 10);
            assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(rejected.error).getCode());
            assertEquals(0, state.getCurrentBallot());
            assertTrue(state.paxos_log.getAcceptedEntries().isEmpty());
            assertTrue(state.notifications.isEmpty());
            RecordingObserver concrete = new RecordingObserver();
            new DidaTradePaxosServiceImpl(state).phasetwo(DidaTradePaxos.PhaseTwoRequest.newBuilder()
                    .setInstance(0).setRequestballot(0).setValue(101).build(), concrete);
            assertTrue(concrete.reply.getAccepted());
            assertEquals(List.of(new Vote(0, 0, 101, 0)), state.notifications);
        }
    }

    private static void checkVotes(boolean collision) {
        PaxosLog learner = new PaxosLog();
        List<Integer> members = List.of(0, 1, 2, 3, 4);
        for (int id = 0; id < 5; id++) {
            LocalState state = new LocalState(id, ProtocolMode.FAST);
            int chosen = collision && id % 2 != 0 ? 102 : 101;
            pending(state, chosen);
            assertAuthorized(any(state, 0, 0));
            state.loop.proposePendingRequests(0);
            pending(state, chosen == 101 ? 102 : 101);
            assertEquals(1, state.notifications.size());
            Vote vote = state.notifications.get(0);
            var result = learner.recordFastLearn(vote.position(), vote.ballot(), vote.value(), id, 4, members);
            if (id < 3) assertEquals(PaxosLog.FastLearnResult.PENDING, result);
            else assertEquals(collision ? PaxosLog.FastLearnResult.COLLISION : PaxosLog.FastLearnResult.DECIDED, result);
            assertNotNull(state.req_history.getIfPending(101));
            assertNotNull(state.req_history.getIfPending(102));
        }
        if (collision) assertFalse(learner.isDecided(0));
        else assertEquals(101, learner.getDecidedCommandId(0).intValue());
    }

    private static void checkPositions() {
        for (int id = 0; id < 2; id++) {
            LocalState state = new LocalState(id, ProtocolMode.FAST);
            pending(state, 101);
            if (id == 1) state.paxos_log.decideEntry(0, 0, 202);
            assertAuthorized(any(state, id, 0));
            state.loop.proposePendingRequests(0);
            assertEquals(List.of(new Vote(id, 0, 101, id)), state.notifications);
        }
    }

    private static void checkReopen() {
        LocalState state = new LocalState(1, ProtocolMode.FAST);
        RequestRecord request = pending(state, 101);
        assertAuthorized(any(state, 0, 0));
        state.loop.proposePendingRequests(0);
        assertEquals(1, state.notifications.size());
        assertAuthorized(any(state, 0, 1));
        assertEquals(101, state.paxos_log.getAcceptedEntries().get(0).getValue());
        assertEquals(0, state.paxos_log.getAcceptedEntries().get(0).getValballot());
        state.loop.proposePendingRequests(1);
        assertEquals(List.of(new Vote(0, 0, 101, 1), new Vote(0, 1, 101, 1)), state.notifications);
        assertSame(request, state.req_history.getIfPending(101));
        assertEquals(1, state.paxos_log.getAcceptedEntries().get(0).getValballot());
        assertAuthorized(any(state, 1, 1));
        state.loop.proposePendingRequests(1);
        assertEquals(2, state.notifications.size()); // Current vote remains a reservation.
        pending(state, 102);
        state.loop.proposePendingRequests(1);
        assertEquals(new Vote(1, 1, 102, 1), state.notifications.get(2));
    }

    private record Vote(int position, int ballot, int value, int acceptor) { }

    private static final class LocalState extends DidaTradeServerState {
        private final FastPaxosMainLoop loop;
        private final List<Vote> notifications = new ArrayList<>();

        private LocalState(int id, ProtocolMode mode) {
            super(19000, id, 'B', mode, false);
            loop = mode == ProtocolMode.FAST ? (FastPaxosMainLoop) main_loop : null;
        }

        @Override public void notifyLearners(int position, int ballot, int value) {
            assertFalse(Thread.holdsLock(this));
            assertFalse(Thread.holdsLock(paxos_log));
            notifications.add(new Vote(position, ballot, value, my_id));
        }
    }

    private static final class RecordingObserver implements StreamObserver<DidaTradePaxos.PhaseTwoReply> {
        private DidaTradePaxos.PhaseTwoReply reply;
        private Throwable error;
        private boolean completed;
        @Override public void onNext(DidaTradePaxos.PhaseTwoReply value) { reply = value; }
        @Override public void onError(Throwable failure) { error = failure; }
        @Override public void onCompleted() { completed = true; }
    }
}
