package didatrade.server;

import junit.framework.TestCase;

import java.util.List;

import static didatrade.server.PaxosLog.FastLearnResult.COLLISION;
import static didatrade.server.PaxosLog.FastLearnResult.DECIDED;
import static didatrade.server.PaxosLog.FastLearnResult.PENDING;

public class FastLearnerTest extends TestCase {
    private static final List<Integer> ACCEPTORS = List.of(0, 1, 2, 3, 4);

    private static PaxosLog.FastLearnResult vote(PaxosLog log, int ballot, int value, int acceptor) {
        return log.recordFastLearn(0, ballot, value, acceptor, 4, ACCEPTORS);
    }

    public void testFourEqualVotesDecideAndDuplicateVotesDoNotCount() {
        PaxosLog log = new PaxosLog();
        assertEquals(PENDING, vote(log, 1, 10, 0));
        assertEquals(PENDING, vote(log, 1, 10, 0));
        assertEquals(PENDING, vote(log, 1, 10, 1));
        assertEquals(PENDING, vote(log, 1, 10, 2));
        assertFalse(log.isDecided(0));
        assertEquals(DECIDED, vote(log, 1, 10, 3));
        assertEquals(DECIDED, vote(log, 1, 10, 3));
        assertEquals(10, log.getDecidedCommandId(0).intValue());
    }

    public void testDivergentVotesProveCollisionOnlyWhenFourVotesAreImpossible() {
        PaxosLog log = new PaxosLog();
        assertEquals(PENDING, vote(log, 1, 10, 0));
        assertEquals(PENDING, vote(log, 1, 20, 1));
        assertEquals(PENDING, vote(log, 1, 10, 2));
        assertEquals(COLLISION, vote(log, 1, 20, 3));
        assertEquals(COLLISION, vote(log, 1, 10, 4));
        assertFalse(log.isDecided(0));
    }

    public void testMinorityDivergenceDoesNotPreventFourEqualVotes() {
        PaxosLog log = new PaxosLog();
        assertEquals(PENDING, vote(log, 1, 20, 4));
        for (int acceptor = 0; acceptor < 3; acceptor++) {
            assertEquals(PENDING, vote(log, 1, 10, acceptor));
        }
        assertEquals(DECIDED, vote(log, 1, 10, 3));
        assertEquals(10, log.getDecidedCommandId(0).intValue());
    }

    public void testChangingVoteIsProtocolErrorAndDoesNotRewriteEvidence() {
        PaxosLog log = new PaxosLog();
        vote(log, 1, 10, 0);
        try {
            vote(log, 1, 20, 0);
            fail("One acceptor cannot change its vote in the same round");
        } catch (IllegalStateException expected) {
            assertEquals(PENDING, vote(log, 1, 10, 0));
        }
        for (int id = 1; id < 3; id++) vote(log, 1, 10, id);
        assertEquals(DECIDED, vote(log, 1, 10, 3));
    }

    public void testVotesRemainSeparatedByPositionAndBallot() {
        PaxosLog log = new PaxosLog();
        for (int id = 0; id < 3; id++) {
            assertEquals(PENDING, vote(log, 1, 10, id));
            assertEquals(PENDING, vote(log, 2, 10, id));
            assertEquals(PENDING, log.recordFastLearn(1, 1, 10, id, 4, ACCEPTORS));
        }
        assertEquals(DECIDED, vote(log, 2, 10, 3));
        assertFalse(log.isDecided(1));
    }

    public void testLateLosingVotesAreToleratedButConflictingCertificateIsRejected() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.accept(0, 3, 30));
        log.decideEntry(0, 2, 20);
        for (int id = 0; id < 3; id++) assertEquals(PENDING, vote(log, 1, 10, id));
        assertEquals(PENDING, vote(log, 1, 40, 3)); // Old collision cannot reopen decision.
        for (int id = 0; id < 3; id++) assertEquals(PENDING, vote(log, 0, 10, id));
        try {
            vote(log, 0, 10, 3);
            fail("A conflicting complete certificate must be rejected");
        } catch (IllegalStateException expected) {
            assertEquals(20, log.getDecidedCommandId(0).intValue());
            assertEquals(30, log.getAcceptedEntries().get(0).getValue());
            assertEquals(3, log.getAcceptedEntries().get(0).getValballot());
        }
    }

    public void testDelayedCompleteCertificateCanDecideAnOlderBallot() {
        PaxosLog log = new PaxosLog();
        for (int id = 0; id < 3; id++) vote(log, 1, 10, id);
        for (int id = 0; id < 2; id++) vote(log, 2, 20, id);
        assertEquals(DECIDED, vote(log, 1, 10, 3));
        assertEquals(10, log.getDecidedCommandId(0).intValue());
    }

    public void testInvalidMembershipAndConfigurationDoNotContributeVotes() {
        PaxosLog log = new PaxosLog();
        assertInvalid(log, 0, 1, 99, 4, ACCEPTORS);
        assertInvalid(log, -1, 1, 0, 4, ACCEPTORS);
        assertInvalid(log, 0, -1, 0, 4, ACCEPTORS);
        assertInvalid(log, 0, 1, 0, 2, ACCEPTORS);
        assertInvalid(log, 0, 1, 0, 6, ACCEPTORS);
        assertInvalid(log, 0, 1, 0, 4, List.of(0, 1, 2, 3, 3));
        assertInvalid(log, 0, 1, 0, 1, List.of());
        for (int id = 0; id < 3; id++) assertEquals(PENDING, vote(log, 1, 10, id));
        assertFalse(log.isDecided(0));
    }

    private static void assertInvalid(PaxosLog log, int position, int ballot, int acceptor,
                                      int quorum, List<Integer> members) {
        try {
            log.recordFastLearn(position, ballot, 10, acceptor, quorum, members);
            fail("Invalid vote/configuration must be rejected");
        } catch (IllegalArgumentException expected) {
            // Validation must happen before recording any evidence.
        }
    }

    public void testAllFiveAcceptorSnapshotsAgreeWithDecisionAndCollisionRules() {
        // Each acceptor has no vote, X or Y: all 3^5 snapshots, including prefixes.
        for (int snapshot = 0; snapshot < 243; snapshot++) {
            PaxosLog log = new PaxosLog();
            int encoded = snapshot;
            int x = 0;
            int y = 0;
            for (int id = 0; id < 5; id++) {
                int value = encoded % 3;
                encoded /= 3;
                if (value == 0) continue;
                if (value == 1) x++; else y++;
                PaxosLog.FastLearnResult result = vote(log, 1, value, id);
                if (Math.max(x, y) >= 4) {
                    assertEquals(DECIDED, result);
                    assertEquals(x >= 4 ? 1 : 2, log.getDecidedCommandId(0).intValue());
                } else if (Math.max(x, y) + 5 - x - y < 4) {
                    assertEquals(COLLISION, result);
                } else {
                    assertEquals(PENDING, result);
                }
            }
        }
    }
}
