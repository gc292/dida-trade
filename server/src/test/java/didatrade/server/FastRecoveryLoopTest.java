package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.FastPhaseOneResponseProcessor;
import didatrade.util.GenericResponseCollector;
import didatrade.util.PhaseTwoAttempt;
import didatrade.util.PhaseTwoResponseProcessor;
import junit.framework.TestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FastRecoveryLoopTest extends TestCase {
    public void testPreparationAndConcreteAttemptLifecycle() throws Exception {
        runIsolated("lifecycle");
    }

    public void testPhaseOneRetriesAreTimedAndAbandonedAfterHigherPromise() throws Exception {
        runIsolated("phaseone");
    }

    public void testRecoveryFillsHolesAroundMissingPayloadAndOutOfOrderDecision() throws Exception {
        runIsolated("holes");
    }

    public void testOldCertificateSurvivesOverlappingNewPreparation() throws Exception {
        runIsolated("overlap");
    }

    public void testImmediateCompletionPublishesWorkAfterAttemptRegistration() throws Exception {
        runIsolated("callback");
    }

    public void testAllProposalWrappersShareAtomicBallotAndDecisionGuards() throws Exception {
        runIsolated("guards");
    }

    public void testAnyWindowRetriesSamePositionAndAdvancesOnlyAfterDecision() throws Exception {
        runIsolated("opening");
    }

    public void testOldAnySuccessCannotAuthorizeNewBallotAndRecoveryBlocksOpening() throws Exception {
        runIsolated("old-any");
    }

    public void testExecutionWaitsForPrefixAndPayloadAndConsumesDuplicateIds() throws Exception {
        runIsolated("execution");
    }

    public void testLearnerDecisionReleasesLosersBeforeLocalResponseAndMissingPayload() throws Exception {
        runIsolated("release-losers");
    }

    public void testLearnerDecisionReleasesRecoveryBeforeOpeningNextSlot() throws Exception {
        runIsolated("release-opening");
    }

    public void testNewConcreteRecoveryReleasesOldCoordinatorChoiceBeforeDecision() throws Exception {
        runIsolated("superseded-concrete");
    }

    public void testNewAnyReleasesBothCoordinatorAndHistoricalVoteReservations() throws Exception {
        runIsolated("superseded-any");
    }

    public void testWindowAboveOneRetainsHolesAndDoesNotReopenInflightSlots() throws Exception {
        runIsolated("window");
    }

    public void testCollisionEventsAdvanceOnlyTheCurrentUndecidedBallot() throws Exception {
        runIsolated("collisions");
    }

    public void testEventLoopPreparesWithoutClientsRetriesSleepsAndVotesAsFollower() throws Exception {
        runIsolated("events");
    }

    public void testRecoveryCanReuseTheOnlyPendingValueWhoseOldVoteWasOutsideTheQuorum() throws Exception {
        runIsolated("recovery-hole");
    }

    public void testLeaderNotificationRetriesWithoutAdvancingAnotherBallot() throws Exception {
        runIsolated("leader-notify");
    }

    public void testLateAnyRecipientFailureRetriesAnAlreadyOpenedSlot() throws Exception {
        runIsolated("late-any");
    }

    public void testAuthorizationRetryWaitsForTheInflightAttemptInsteadOfSpinning() throws Exception {
        runIsolated("inflight-retry");
    }

    private void runIsolated(String scenario) throws Exception {
        Path output = Path.of("target", "fast-recovery-loop-" + scenario + ".log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, FastRecoveryLoopTest.class.getName(), scenario)
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
                case "lifecycle" -> checkLifecycle();
                case "phaseone" -> checkPhaseOneRetry();
                case "holes" -> checkHoles();
                case "overlap" -> checkOverlap();
                case "callback" -> checkImmediateCallback();
                case "guards" -> checkGuards();
                case "opening" -> checkOpening();
                case "old-any" -> checkOldAny();
                case "execution" -> checkExecution();
                case "release-losers" -> checkReleaseLosers();
                case "release-opening" -> checkReleaseOpening();
                case "superseded-concrete" -> checkSupersededRecovery(false);
                case "superseded-any" -> checkSupersededRecovery(true);
                case "inflight-retry" -> checkInflightRetry();
                case "late-any" -> checkLateAny();
                case "leader-notify" -> checkLeaderNotification();
                case "recovery-hole" -> checkRecoveryHole();
                case "window" -> checkWindow();
                case "collisions" -> checkCollisions();
                case "events" -> checkEvents();
                default -> throw new IllegalArgumentException("Unknown scenario");
            }
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static void checkInflightRetry() throws Exception {
        ControlledState state = new ControlledState();
        state.emptyPreparation = true;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        loop.openNextInstances(0);
        loop.onAuthorizationFailure(0, 0);
        loop.processEvents();
        Thread.sleep(550);
        assertFalse(state.anyCollectors.get(0).isDone());
        assertEquals("Only completion can unblock an in-flight proposal", -1L, loop.nextRetryDelayMillis());
        loop.openNextInstances(0);
        assertEquals(1, state.anySent);
        for (int id = 0; id < 4; id++) state.anyCollectors.get(0).addNoResponse();
        loop.processCompletedAttempts();
        assertTrue(loop.nextRetryDelayMillis() > 0);
        Thread.sleep(550);
        loop.openNextInstances(0);
        assertEquals(2, state.anySent);
    }

    private static void checkLateAny() throws Exception {
        ControlledState state = new ControlledState();
        state.emptyPreparation = true;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        loop.openNextInstances(0);
        for (int acceptor = 0; acceptor < 4; acceptor++) completeAny(state, 0, acceptor);
        loop.processCompletedAttempts();
        assertEquals(-1L, loop.nextRetryDelayMillis());
        loop.onAuthorizationFailure(0, 0); // Fifth RPC fails after quorum already completed.
        loop.processEvents();
        loop.openNextInstances(0);
        assertEquals(1, state.anySent);
        Thread.sleep(550);
        loop.openNextInstances(0);
        assertEquals(List.of(new AnySent(0, 0), new AnySent(0, 0)), state.anyProposals);
        for (int acceptor = 0; acceptor < 4; acceptor++) completeAny(state, 1, acceptor);
        loop.processCompletedAttempts();
        assertEquals(-1L, loop.nextRetryDelayMillis());
        state.paxos_log.decideEntry(0, 0, 100);
        loop.onAuthorizationFailure(0, 0);
        loop.processEvents();
        assertEquals(-1L, loop.nextRetryDelayMillis());
        state.setCurrentBallot(1);
        loop.onAuthorizationFailure(1, 0);
        loop.processEvents();
        assertEquals(-1L, loop.nextRetryDelayMillis());
    }

    private static void checkLeaderNotification() throws Exception {
        ControlledState state = new ControlledState();
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        state.main_loop = loop;
        loop.onCollision(0, 0);
        Thread worker = new Thread(loop);
        worker.start();
        try {
            awaitCondition(() -> state.notificationCallbacks.size() == 1);
            assertEquals(1, state.getCurrentBallot());
            state.notificationCallbacks.get(0).accept(false);
            awaitCondition(() -> state.notificationCallbacks.size() == 2);
            assertEquals(List.of(1, 1), state.notifiedBallots);
            assertEquals(1, state.getCurrentBallot());
            assertEquals(-1, state.getCompletedBallot());
            state.notificationCallbacks.get(1).accept(true);
            awaitCondition(() -> worker.getState() == Thread.State.WAITING);
            loop.onCollision(0, 0);
            loop.onCollision(1, 0);
            Thread.sleep(100);
            assertEquals(1, state.getCurrentBallot());
            assertEquals(2, state.notifiedBallots.size());
            assertEquals(0, state.preparations); // New leader is replica 1, not this replica.
        } finally {
            worker.interrupt();
            worker.join(2000);
            assertFalse(worker.isAlive());
        }
    }

    private static void checkRecoveryHole() {
        ControlledState state = new ControlledState();
        state.slot = 1;
        assertTrue(state.acceptPhaseTwo(0, 0, 100).accepted());
        state.setCurrentBallot(3);
        state.req_history.addToPending(100, request(100));
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(3));
        loop.startRecoveredProposals(3);
        assertTrue(state.sent.contains(new Sent(0, 3, 100)));
        assertTrue(state.sent.contains(new Sent(1, 3, 20)));
        // Scheduling a new recovery choice must not erase the historical Phase 1 snapshot.
        assertEquals(100, state.paxos_log.getAcceptedEntries().get(0).getValue());
        assertEquals(0, state.paxos_log.getAcceptedEntries().get(0).getValballot());
    }

    private static void checkWindow() {
        ControlledState state = new ControlledState();
        state.emptyPreparation = true;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state, 3);
        assertTrue(loop.prepareLeadership(0));
        loop.openNextInstances(0);
        loop.openNextInstances(0);
        assertEquals(List.of(new AnySent(0, 0), new AnySent(1, 0), new AnySent(2, 0)), state.anyProposals);
        state.paxos_log.decideEntry(2, 0, 200);
        loop.openNextInstances(0);
        assertEquals(3, state.anySent);
        state.paxos_log.decideEntry(0, 0, 100);
        loop.openNextInstances(0);
        assertEquals(new AnySent(3, 0), state.anyProposals.get(3));
        loop.openNextInstances(0);
        assertEquals(4, state.anySent);
    }

    private static void checkCollisions() {
        ControlledState state = new ControlledState();
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        for (int i = 0; i < 20; i++) {
            loop.onCollision(0, 0);
            loop.onCollision(1, 0);
        }
        assertEquals(0, state.getCurrentBallot()); // Callback only publishes an event.
        loop.processEvents();
        assertEquals(1, state.getCurrentBallot());
        loop.onCollision(1, 0);
        loop.processEvents();
        assertEquals(1, state.getCurrentBallot());
        loop.onCollision(1, 1);
        state.setCurrentBallot(3); // Manual change wins over the queued collision.
        loop.processEvents();
        assertEquals(3, state.getCurrentBallot());
        state.paxos_log.decideEntry(2, 3, 200);
        loop.onCollision(2, 3);
        loop.processEvents();
        assertEquals(3, state.getCurrentBallot());
    }

    private static void checkEvents() throws Exception {
        ControlledState state = new ControlledState();
        state.emptyPreparation = true;
        state.failPreparation = true;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state, 4);
        state.main_loop = loop;
        Thread worker = new Thread(loop);
        worker.start();
        try {
            awaitCondition(() -> state.preparations >= 2);
            assertEquals(0, state.anySent);
            awaitCondition(() -> worker.getState() == Thread.State.TIMED_WAITING);
            state.failPreparation = false;
            awaitCondition(() -> state.anySent == 4 && worker.getState() == Thread.State.WAITING);
            for (int attempt = 0; attempt < 4; attempt++) {
                for (int acceptor = 0; acceptor < 4; acceptor++) completeAny(state, attempt, acceptor);
            }
            awaitCondition(() -> worker.getState() == Thread.State.WAITING);
            int preparations = state.preparations;
            Thread.sleep(100);
            assertEquals(preparations, state.preparations);
            assertEquals(4, state.anySent);
            assertFalse(state.paxos_log.isDecided(0));
            state.setCurrentBallot(1); // This replica is now a follower.
            assertTrue(state.authorizeFastInstance(0, 1).accepted());
            state.req_history.addToPending(100, request(100));
            loop.wakeup();
            awaitCondition(() -> state.localVotes.size() == 1);
            assertEquals(new Sent(0, 1, 100), state.localVotes.get(0));
            assertEquals(4, state.anySent); // A follower does not open a new window.
        } finally {
            worker.interrupt();
            worker.join(2000);
            assertFalse(worker.isAlive());
        }
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) fail("Timed out awaiting loop event");
            Thread.sleep(10);
        }
    }

    private static void checkLifecycle() throws Exception {
        ControlledState state = new ControlledState();
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertFalse(loop.prepareLeadership(1));
        assertEquals(0, state.preparations);
        assertTrue(loop.prepareLeadership(0));
        assertTrue(loop.prepareLeadership(0));
        assertEquals(1, state.preparations);
        loop.startRecoveredProposals(0);
        loop.startRecoveredProposals(0);
        assertEquals(1, state.sent.size());
        assertEquals(20, state.sent.get(0).value());
        loop.processCompletedAttempts();
        assertFalse(state.paxos_log.isDecided(0));

        // Timeout completes without quorum and cannot cause an immediate resend.
        for (int i = 0; i < 4; i++) state.collectors.get(0).addNoResponse();
        assertTrue(loop.takeWork());
        assertFalse(loop.takeWork());
        loop.processCompletedAttempts();
        assertTrue(loop.nextRetryDelayMillis() >= 0);
        loop.startRecoveredProposals(0);
        assertEquals(1, state.sent.size());
        Thread.sleep(550);
        assertEquals(0L, loop.nextRetryDelayMillis());
        loop.startRecoveredProposals(0);
        assertEquals(-1L, loop.nextRetryDelayMillis());
        assertEquals(2, state.sent.size());
        assertEquals(state.sent.get(0), state.sent.get(1));

        // A full old-ballot certificate remains valid after a higher promise.
        state.setCurrentBallot(3);
        complete(state, 1, 0, true, 0);
        loop.processCompletedAttempts();
        assertEquals(20, state.paxos_log.getDecidedCommandId(0).intValue());
        loop.startRecoveredProposals(0);
        assertEquals(2, state.sent.size());

        // Old failures must not retry, even after this node becomes leader again.
        state.slot = 1;
        assertTrue(loop.prepareLeadership(3));
        loop.startRecoveredProposals(3);
        assertEquals(3, state.sent.size());
        state.setCurrentBallot(6);
        complete(state, 2, 3, false, 6);
        loop.processCompletedAttempts();
        Thread.sleep(550);
        loop.startRecoveredProposals(3);
        assertEquals(3, state.sent.size());
        assertFalse(state.paxos_log.isDecided(1));

        // A higher rejection aborts current leadership; it is not a decision.
        assertTrue(loop.prepareLeadership(6));
        loop.startRecoveredProposals(6);
        complete(state, 3, 6, false, 7);
        loop.processCompletedAttempts();
        assertEquals(7, state.getCurrentBallot());
        assertFalse(state.paxos_log.isDecided(1));

        // A promise change while Phase 1 is running invalidates its preparation.
        state.setCurrentBallot(9);
        state.advanceDuringPreparation = true;
        assertFalse(loop.prepareLeadership(9));
        loop.startRecoveredProposals(9);
        assertEquals(4, state.sent.size());
    }

    private static void checkPhaseOneRetry() throws Exception {
        ControlledState state = new ControlledState();
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        state.failPreparation = true;
        assertFalse(loop.prepareLeadership(0));
        assertTrue(loop.nextRetryDelayMillis() >= 0);
        assertFalse(loop.prepareLeadership(0));
        assertEquals(1, state.preparations);
        Thread.sleep(550);
        assertEquals(0L, loop.nextRetryDelayMillis());
        state.failPreparation = false;
        assertTrue(loop.prepareLeadership(0));
        assertEquals(2, state.preparations);
        assertEquals(-1L, loop.nextRetryDelayMillis());
        state.setCurrentBallot(3);
        state.failPreparation = true;
        state.rejectedMaxBallot = 4;
        assertFalse(loop.prepareLeadership(3));
        assertEquals(4, state.getCurrentBallot());
        assertEquals(-1L, loop.nextRetryDelayMillis());
        assertFalse(loop.prepareLeadership(3));
        assertEquals(3, state.preparations);
    }

    private static void checkHoles() throws Exception {
        ControlledState state = new ControlledState();
        state.slot = 2;
        state.paxos_log.decideEntry(4, 0, 40);
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        loop.startRecoveredProposals(0);
        assertEquals(List.of(new Sent(2, 0, 20)), state.sent);
        // Accepted value 20 is recovered without a payload; it is not a free hole.
        state.req_history.addToPending(20, request(20));
        state.req_history.addToPending(100, request(100));
        loop.startRecoveredProposals(0);
        assertTrue(state.sent.contains(new Sent(0, 0, 100)));
        assertEquals(2, state.sent.size());
        state.req_history.addToPending(101, request(101));
        state.req_history.addToPending(102, request(102));
        loop.startRecoveredProposals(0);
        assertEquals(4, state.sent.size());
        assertTrue(state.sent.stream().anyMatch(sent -> sent.position() == 1));
        assertTrue(state.sent.stream().anyMatch(sent -> sent.position() == 3));
        assertFalse(state.sent.stream().anyMatch(sent -> sent.position() == 4));
        assertEquals(4L, state.sent.stream().map(Sent::value).distinct().count());
        int holeAttempt = state.sent.indexOf(new Sent(0, 0, 100));
        for (int i = 0; i < 4; i++) state.collectors.get(holeAttempt).addNoResponse();
        loop.processCompletedAttempts();
        Thread.sleep(550);
        loop.startRecoveredProposals(0);
        assertEquals(new Sent(0, 0, 100), state.sent.get(4));
    }

    private static RequestRecord request(int id) {
        return new RequestRecord(id, new DidaTradeCommand(DidaTradeAction.ADDUSER, 42, 10));
    }

    private static void checkOverlap() {
        ControlledState state = new ControlledState();
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        loop.startRecoveredProposals(0);
        state.setCurrentBallot(3);
        assertTrue(loop.prepareLeadership(3));
        loop.startRecoveredProposals(3);
        assertEquals(2, state.sent.size());
        complete(state, 0, 0, true, 0);
        loop.processCompletedAttempts();
        assertEquals(20, state.paxos_log.getDecidedCommandId(0).intValue());
        complete(state, 1, 3, false, 3);
        loop.processCompletedAttempts();
        assertEquals(-1L, loop.nextRetryDelayMillis());
        loop.startRecoveredProposals(3);
        assertEquals(2, state.sent.size());
    }

    private static void checkImmediateCallback() {
        ControlledState state = new ControlledState();
        state.completeDuringSend = true;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertFalse(loop.takeWork());
        assertTrue(loop.prepareLeadership(0));
        loop.startRecoveredProposals(0);
        assertTrue(loop.takeWork());
        assertFalse(loop.takeWork());
        loop.processCompletedAttempts();
        assertEquals(20, state.paxos_log.getDecidedCommandId(0).intValue());
        loop.startRecoveredProposals(0);
        assertEquals(1, state.sent.size());
    }

    private static void checkGuards() {
        ControlledState state = new ControlledState();
        assertNull(state.startPhaseTwoIfCurrentAndUndecided(0, 1, 20));
        assertNull(state.startFastPhaseTwoIfCurrentAndUndecided(0, 1, 20));
        assertNull(state.startFastAnyIfCurrentAndUndecided(0, 1));
        assertTrue(state.sent.isEmpty());
        assertEquals(0, state.anySent);

        assertNotNull(state.startPhaseTwoIfCurrentAndUndecided(0, 0, 20));
        assertNotNull(state.startFastPhaseTwoIfCurrentAndUndecided(1, 0, 20));
        assertNotNull(state.startFastAnyIfCurrentAndUndecided(2, 0));
        assertEquals(2, state.sent.size());
        assertEquals(1, state.anySent);

        state.paxos_log.decideEntry(2, 0, 20);
        assertNull(state.startPhaseTwoIfCurrentAndUndecided(2, 0, 20));
        assertNull(state.startFastPhaseTwoIfCurrentAndUndecided(2, 0, 20));
        assertNull(state.startFastAnyIfCurrentAndUndecided(2, 0));
        state.setCurrentBallot(3);
        assertNull(state.startPhaseTwoIfCurrentAndUndecided(0, 0, 20));
        assertNull(state.startFastPhaseTwoIfCurrentAndUndecided(0, 0, 20));
        assertNull(state.startFastAnyIfCurrentAndUndecided(0, 0));
        assertEquals(2, state.sent.size());
        assertEquals(1, state.anySent);
        assertNotNull(state.startFastAnyIfCurrentAndUndecided(3, 3));
        assertEquals(2, state.anySent);
    }

    private static void checkOpening() throws Exception {
        ControlledState state = new ControlledState();
        state.emptyPreparation = true;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        loop.openNextInstances(0);
        assertEquals(0, state.anySent);
        assertTrue(loop.prepareLeadership(0));
        loop.openNextInstances(0);
        loop.openNextInstances(0);
        assertEquals(1, state.anySent);
        for (int id = 0; id < 3; id++) completeAny(state, 0, id);
        state.anyCollectors.get(0).addNoResponse();
        loop.processCompletedAttempts();
        loop.openNextInstances(0);
        assertEquals(1, state.anySent);
        Thread.sleep(550);
        loop.openNextInstances(0);
        assertEquals(2, state.anySent);
        assertEquals(state.anyProposals.get(0), state.anyProposals.get(1));
        for (int id = 0; id < 4; id++) completeAny(state, 1, id);
        assertTrue(loop.takeWork());
        loop.processCompletedAttempts();
        assertFalse(state.paxos_log.isDecided(0));
        assertTrue(state.paxos_log.getAcceptedEntries().isEmpty());
        assertEquals(-1, state.getCompletedBallot());
        loop.openNextInstances(0);
        assertEquals(2, state.anySent);
        state.paxos_log.decideEntry(0, 0, 20);
        loop.openNextInstances(0);
        assertEquals(3, state.anySent);
        assertEquals(new AnySent(1, 0), state.anyProposals.get(2));
    }

    private static void checkOldAny() {
        ControlledState state = new ControlledState();
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        loop.openNextInstances(0);
        assertEquals(0, state.anySent); // Accepted recovery slot must stay concrete.
        state.emptyPreparation = true;
        state.setCurrentBallot(3);
        assertTrue(loop.prepareLeadership(3));
        loop.openNextInstances(3);
        assertEquals(1, state.anySent);
        state.setCurrentBallot(6);
        assertTrue(loop.prepareLeadership(6));
        for (int id = 0; id < 4; id++) completeAny(state, 0, id);
        loop.processCompletedAttempts();
        assertFalse(state.paxos_log.isDecided(0));
        loop.openNextInstances(6);
        assertEquals(2, state.anySent);
        assertEquals(new AnySent(0, 6), state.anyProposals.get(1));
    }

    private static void completeAny(ControlledState state, int attempt, int acceptor) {
        AnySent proposal = state.anyProposals.get(attempt);
        state.anyCollectors.get(attempt).addResponse(DidaTradePaxos.PhaseTwoReply.newBuilder()
                .setServerid(acceptor).setInstance(proposal.position()).setRequestballot(proposal.ballot())
                .setAccepted(true).setMaxballot(proposal.ballot()).build());
    }

    private static void checkExecution() {
        ControlledState state = new ControlledState();
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        for (int id : new int[] {101, 102, 104}) state.req_history.addToPending(id, request(id));
        state.paxos_log.decideEntry(1, 0, 102);
        loop.executeDecidedPrefix();
        assertTrue(state.executed.isEmpty());
        state.paxos_log.decideEntry(0, 0, 101);
        loop.executeDecidedPrefix();
        assertEquals(List.of(101, 102), state.executed);
        assertNotNull(state.req_history.getIfProcessed(101));
        assertNotNull(state.req_history.getIfProcessed(102));
        state.paxos_log.decideEntry(2, 0, 101);
        state.paxos_log.decideEntry(3, 0, 103); // Missing payload stops the prefix.
        state.paxos_log.decideEntry(4, 0, 104);
        loop.executeDecidedPrefix();
        assertEquals(List.of(101, 102), state.executed);
        assertNotNull(state.req_history.getIfPending(104));
        state.req_history.addToPending(103, request(103));
        loop.executeDecidedPrefix();
        assertEquals(List.of(101, 102, 103, 104), state.executed);

        state.setCurrentBallot(3);
        assertTrue(loop.prepareLeadership(3)); // Must not reset the execution cursor.
        state.req_history.addToPending(105, request(105));
        state.paxos_log.decideEntry(5, 3, 105);
        loop.executeDecidedPrefix();
        loop.executeDecidedPrefix();
        assertEquals(List.of(101, 102, 103, 104, 105), state.executed);
        assertNotNull(state.req_history.getIfProcessed(105));
    }

    private static void checkReleaseLosers() {
        ControlledState state = new ControlledState();
        state.slot = 2;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        for (int id : new int[] {20, 100, 200}) state.req_history.addToPending(id, request(id));
        loop.startRecoveredProposals(0);
        assertEquals(3, state.sent.size());
        // The old leader becomes a follower; decisions precede its RPC completions.
        state.setCurrentBallot(1);
        state.paxos_log.decideEntry(0, 1, 900);
        state.paxos_log.decideEntry(1, 1, 901);
        state.paxos_log.decideEntry(2, 1, 20);
        assertTrue(state.authorizeFastInstance(3, 1).accepted());
        assertTrue(state.authorizeFastInstance(4, 1).accepted());
        // Payload for 900 is missing, but that must not keep losing reservations.
        loop.proposePendingRequests(1);
        assertEquals(List.of(new Sent(3, 1, 100), new Sent(4, 1, 200)), state.localVotes);
        assertNotNull(state.req_history.getIfPending(100));
        assertNotNull(state.req_history.getIfPending(200));
        loop.executeDecidedPrefix();
        assertTrue(state.executed.isEmpty());

        // Cleanup retains old in-flight attempts, so their certificates are checked.
        complete(state, 2, 0, true, 0);
        loop.processCompletedAttempts();
        assertEquals(20, state.paxos_log.getDecidedCommandId(2).intValue());
        complete(state, 0, 0, true, 0);
        try {
            loop.processCompletedAttempts();
            fail("A conflicting old certificate must still be checked after reservation release");
        } catch (IllegalStateException expected) {
            assertEquals(900, state.paxos_log.getDecidedCommandId(0).intValue());
        }
    }

    private static void checkReleaseOpening() {
        ControlledState state = new ControlledState();
        state.slot = 2;
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        loop.startRecoveredProposals(0);
        loop.openNextInstances(0);
        assertEquals(0, state.anySent);
        state.paxos_log.decideEntry(0, 0, 100);
        state.paxos_log.decideEntry(1, 0, 200);
        state.paxos_log.decideEntry(2, 0, 20);
        // The attempt for slot 2 is still pending; recovery maps must not block ANY.
        loop.openNextInstances(0);
        assertEquals(List.of(new AnySent(3, 0)), state.anyProposals);
        complete(state, 0, 0, true, 0);
        loop.processCompletedAttempts();
        assertEquals(20, state.paxos_log.getDecidedCommandId(2).intValue());
    }

    private static void checkSupersededRecovery(boolean any) {
        ControlledState state = new ControlledState();
        assertTrue(state.acceptPhaseTwo(0, 0, 20).accepted());
        FastPaxosMainLoop loop = new FastPaxosMainLoop(state);
        assertTrue(loop.prepareLeadership(0));
        state.req_history.addToPending(20, request(20));
        state.req_history.addToPending(100, request(100));
        loop.startRecoveredProposals(0);
        assertEquals(1, state.sent.size());
        if (any) {
            assertTrue(state.authorizeFastInstance(0, 1).accepted());
            assertEquals(20, state.paxos_log.getAcceptedEntries().get(0).getValue());
            assertEquals(0, state.paxos_log.getAcceptedEntries().get(0).getValballot());
        } else {
            assertTrue(state.acceptPhaseTwo(0, 1, 900).accepted());
            assertTrue(state.authorizeFastInstance(1, 1).accepted());
        }
        loop.proposePendingRequests(1);
        assertEquals(List.of(new Sent(any ? 0 : 1, 1, 20)), state.localVotes);
        assertNotNull(state.req_history.getIfPending(20));
        assertFalse(state.paxos_log.isDecided(0));
        assertTrue(state.authorizeFastInstance(any ? 1 : 2, 1).accepted());
        loop.proposePendingRequests(1);
        assertEquals(new Sent(any ? 1 : 2, 1, 100), state.localVotes.get(1));
        // Old in-flight proposal is retained; failures cannot recreate its old reservation.
        for (int i = 0; i < 4; i++) state.collectors.get(0).addNoResponse();
        loop.processCompletedAttempts();
        assertEquals(-1L, loop.nextRetryDelayMillis());
        assertEquals(1, state.sent.size());
    }

    private static void complete(ControlledState state, int attempt, int ballot, boolean accepted, int maxBallot) {
        for (int id = 0; id < (accepted ? 4 : 1); id++) {
            state.collectors.get(attempt).addResponse(DidaTradePaxos.PhaseTwoReply.newBuilder()
                    .setServerid(id).setRequestballot(ballot).setAccepted(accepted).setMaxballot(maxBallot).build());
        }
    }

    private record Sent(int position, int ballot, int value) { }
    private record AnySent(int position, int ballot) { }

    // Replace only transport; exercise real promise checks, log and loop methods.
    private static final class ControlledState extends DidaTradeServerState {
        private volatile int preparations;
        private int slot;
        private boolean advanceDuringPreparation;
        private volatile boolean failPreparation;
        private int rejectedMaxBallot = -1;
        private boolean completeDuringSend;
        private volatile int anySent;
        private boolean emptyPreparation;
        private final List<AnySent> anyProposals = new ArrayList<>();
        private final List<GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>> anyCollectors = new ArrayList<>();
        private final List<Sent> sent = new ArrayList<>();
        private final List<Integer> executed = new ArrayList<>();
        private final List<Sent> localVotes = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final List<GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>> collectors = new ArrayList<>();

        private final List<Integer> notifiedBallots = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final List<java.util.function.Consumer<Boolean>> notificationCallbacks =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override void notifyFastLeader(int ballot, java.util.function.Consumer<Boolean> callback) {
            assertFalse(Thread.holdsLock(this));
            notifiedBallots.add(ballot);
            notificationCallbacks.add(callback);
        }

        private ControlledState() { super(19000, 0, 'B', ProtocolMode.FAST, false); }

        @Override public void executeRequest(RequestRecord request) {
            executed.add(request.getId());
            super.executeRequest(request);
        }

        @Override public void notifyLearners(int position, int ballot, int value) {
            assertFalse(Thread.holdsLock(this));
            assertFalse(Thread.holdsLock(paxos_log));
            localVotes.add(new Sent(position, ballot, value));
        }

        @Override public FastPhaseOneResponseProcessor runFastPhaseOne(int ballot) {
            preparations++;
            var processor = new FastPhaseOneResponseProcessor(ballot, scheduler.acceptors(ballot), 4, 4);
            if (failPreparation) {
                processor.onNext(new ArrayList<>(), DidaTradePaxos.PhaseOneReply.newBuilder()
                        .setServerid(0).setRequestballot(ballot).setAccepted(false)
                        .setMaxballot(Math.max(ballot, rejectedMaxBallot)).build());
                return processor;
            }
            for (int id = 0; id < 4; id++) {
                var response = DidaTradePaxos.PhaseOneReply.newBuilder().setServerid(id)
                        .setRequestballot(ballot).setAccepted(true).setMaxballot(ballot);
                if (!emptyPreparation) {
                    response.addAcceptedEntries(DidaTradePaxos.AcceptedEntry.newBuilder()
                            .setInstance(slot).setValue(20).setValballot(Math.max(0, ballot - 1)));
                }
                processor.onNext(new ArrayList<>(), response.build());
            }
            if (advanceDuringPreparation) setCurrentBallot(ballot + 1);
            return processor;
        }

        @Override public PhaseTwoAttempt startFastPhaseTwo(int position, int ballot, int value) {
            assertTrue(Thread.holdsLock(this));
            assertTrue(Thread.holdsLock(paxos_log));
            sent.add(new Sent(position, ballot, value));
            var processor = new PhaseTwoResponseProcessor(4);
            GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector =
                    new GenericResponseCollector<>(new ArrayList<>(), 4, processor);
            collectors.add(collector);
            if (completeDuringSend) complete(this, collectors.size() - 1, ballot, true, ballot);
            return new PhaseTwoAttempt(collector, processor);
        }

        @Override public PhaseTwoAttempt startPhaseTwo(int position, int ballot, int value) {
            return startFastPhaseTwo(position, ballot, value);
        }

        @Override public PhaseTwoAttempt startFastAny(int position, int ballot) {
            assertTrue(Thread.holdsLock(this));
            assertTrue(Thread.holdsLock(paxos_log));
            anySent++;
            anyProposals.add(new AnySent(position, ballot));
            var processor = new PhaseTwoResponseProcessor(4);
            GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector =
                    new GenericResponseCollector<>(new ArrayList<>(), 4, processor);
            anyCollectors.add(collector);
            return new PhaseTwoAttempt(collector, processor);
        }
    }
}
