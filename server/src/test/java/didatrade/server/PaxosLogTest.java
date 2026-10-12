package didatrade.server;

import junit.framework.TestCase;

public class PaxosLogTest extends TestCase {
    public void testDecisionDoesNotRewriteAcceptedSnapshot() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.accept(0, 1, 10));
        log.decideEntry(0, 2, 20);
        assertEquals(20, log.getDecidedCommandId(0).intValue());
        assertEquals(10, log.getAcceptedEntries().get(0).getValue());
        assertEquals(1, log.getAcceptedEntries().get(0).getValballot());
    }

    public void testDelayedLosingVoteAfterDecision() {
        PaxosLog log = new PaxosLog();
        assertFalse(log.recordLearn(0, 0, 10, 0, 2));
        assertFalse(log.recordLearn(0, 1, 20, 1, 2));
        assertTrue(log.recordLearn(0, 1, 20, 2, 2));
        assertFalse(log.recordLearn(0, 0, 10, 0, 2));
        assertEquals(20, log.getDecidedCommandId(0).intValue());
        try {
            log.decideEntry(0, 2, 10);
            fail("Conflicting decisions must still be rejected");
        } catch (IllegalStateException expected) {
            // A decision is stronger evidence than an isolated delayed vote.
        }
    }

    public void testVotesAreDistinctAndBallotsRemainSeparate() {
        PaxosLog log = new PaxosLog();
        assertFalse(log.recordLearn(1, 0, 10, 0, 2));
        assertFalse(log.recordLearn(1, 0, 10, 0, 2));
        assertFalse(log.recordLearn(1, 1, 10, 1, 2));
        assertTrue(log.recordLearn(1, 0, 10, 2, 2));
        assertEquals(-1, log.findMaxDecidedBallot());
        log.decideEntry(0, 0, 20);
        assertEquals(0, log.findMaxDecidedBallot());
    }

    public void testAcceptorDoesNotChangeItsVoteInTheSameBallot() {
        PaxosLog log = new PaxosLog();
        assertTrue(log.accept(0, 3, 10));
        assertTrue(log.accept(0, 3, 10));
        assertFalse(log.accept(0, 3, 20));
        assertFalse(log.accept(0, 2, 10));
        assertTrue(log.accept(0, 4, 20));
        assertEquals(20, log.getAcceptedEntries().get(0).getValue());
    }
}
