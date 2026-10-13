package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.FastPhaseOneResponseProcessor;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FastPhaseOneResponseProcessorTest extends TestCase {
    private static FastPhaseOneResponseProcessor processor(int n, int quorum) {
        List<Integer> acceptors = new ArrayList<>();
        for (int id = 0; id < n; id++) acceptors.add(id);
        return new FastPhaseOneResponseProcessor(10, acceptors, quorum);
    }

    private static DidaTradePaxos.AcceptedEntry entry(int position, int value, int ballot) {
        return DidaTradePaxos.AcceptedEntry.newBuilder().setInstance(position)
                .setValue(value).setValballot(ballot).build();
    }

    private static DidaTradePaxos.PhaseOneReply reply(int id, int ballot, boolean accepted,
                                                   int maxBallot, DidaTradePaxos.AcceptedEntry... entries) {
        return DidaTradePaxos.PhaseOneReply.newBuilder().setServerid(id).setRequestballot(ballot)
                .setAccepted(accepted).setMaxballot(maxBallot).addAllAcceptedEntries(List.of(entries)).build();
    }

    private static boolean feed(FastPhaseOneResponseProcessor processor, int id,
                                DidaTradePaxos.AcceptedEntry... entries) {
        return processor.onNext(new ArrayList<>(), reply(id, 10, true, 10, entries));
    }

    private static void assertNoRecovery(FastPhaseOneResponseProcessor processor) {
        try {
            processor.getRecoveredEntries();
            fail("Recovery requires a successful quorum");
        } catch (IllegalStateException expected) {
            assertFalse(processor.getAccepted());
        }
    }

    public void testOnlyDistinctPromisesFromThisBallotAndConfigurationCount() {
        var processor = processor(3, 3);
        assertNoRecovery(processor);
        assertFalse(processor.onNext(new ArrayList<>(), reply(99, 10, false, 90)));
        assertFalse(processor.onNext(new ArrayList<>(), reply(0, 9, false, 90)));
        assertEquals(-1, processor.getMaxballot());
        assertFalse(feed(processor, 0, entry(0, 20, 7)));
        assertFalse(feed(processor, 0, entry(0, 10, 8)));
        assertFalse(feed(processor, 1));
        assertNoRecovery(processor);
        assertTrue(feed(processor, 2));
        assertTrue(processor.getAccepted());
        assertEquals(20, processor.getRecoveredEntries().get(0).getValue());
        assertEquals(7, processor.getRecoveredEntries().get(0).getValballot());
    }

    public void testRejectionEndsAttemptWithoutQuorumAndRetainsHigherPromise() {
        var processor = processor(3, 3);
        assertFalse(feed(processor, 0));
        assertTrue(processor.onNext(new ArrayList<>(), reply(1, 10, false, 12)));
        assertFalse(processor.getAccepted());
        assertEquals(12, processor.getMaxballot());
        assertTrue(feed(processor, 2));
        assertTrue(feed(processor, 1));
        assertNoRecovery(processor);
    }

    public void testThreeAcceptorRecoveryIsIndependentPerPosition() {
        var processor = processor(3, 3);
        feed(processor, 0, entry(0, 20, 7), entry(1, 20, 7), entry(2, 20, 8), entry(3, 30, 7));
        feed(processor, 1, entry(0, 20, 7), entry(1, 10, 7), entry(2, 10, 7));
        feed(processor, 2, entry(0, 20, 7), entry(1, 20, 7), entry(2, 10, 7));
        var recovered = processor.getRecoveredEntries();
        assertEquals(4, recovered.size());
        assertEquals(20, recovered.get(0).getValue()); // Unanimous.
        assertEquals(10, recovered.get(1).getValue()); // Stable tie, not plurality.
        assertEquals(20, recovered.get(2).getValue()); // Highest ballot, not frequency.
        assertEquals(8, recovered.get(2).getValballot());
        assertEquals(30, recovered.get(3).getValue()); // Single reported candidate.
        assertFalse(recovered.containsKey(4));
    }

    public void testEmptyPromisesHaveNoConcreteRecovery() {
        var processor = processor(3, 3);
        feed(processor, 0);
        feed(processor, 1);
        feed(processor, 2);
        assertTrue(processor.getAccepted());
        assertTrue(processor.getRecoveredEntries().isEmpty());
    }

    public void testFiveAcceptorSlideExamplesAndFrozenQuorum() {
        var processor = processor(5, 4);
        feed(processor, 0, entry(0, 20, 6), entry(1, 20, 6), entry(3, 20, 6));
        feed(processor, 1, entry(0, 20, 6), entry(1, 20, 6), entry(3, 30, 6));
        feed(processor, 2, entry(0, 20, 6), entry(1, 10, 6), entry(3, 20, 6));
        assertTrue(feed(processor, 3, entry(0, 10, 6), entry(1, 10, 6), entry(3, 10, 4)));
        Map<Integer, DidaTradePaxos.AcceptedEntry> before = processor.getRecoveredEntries();
        assertEquals(20, before.get(0).getValue()); // Three votes force B over smaller A.
        assertEquals(10, before.get(1).getValue()); // Two each: stable choice A.
        assertFalse(before.containsKey(2)); // No votes in Q, even if outside Q voted.
        assertEquals(20, before.get(3).getValue()); // B or C at 6; A at 4 excluded.
        assertTrue(feed(processor, 4, entry(0, 10, 9), entry(2, 10, 6)));
        assertTrue(processor.onNext(new ArrayList<>(), reply(4, 10, false, 12)));
        assertEquals(before, processor.getRecoveredEntries());
        assertEquals(10, processor.getMaxballot());
        assertTrue(processor.getAccepted());
        before.clear();
        assertEquals(3, processor.getRecoveredEntries().size());
    }

    public void testArrivalOrderDoesNotChangeTieBreak() {
        var first = processor(3, 3);
        var second = processor(3, 3);
        feed(first, 0, entry(0, 30, 7));
        feed(first, 1, entry(0, 10, 7));
        feed(first, 2, entry(0, 20, 7));
        feed(second, 2, entry(0, 20, 7));
        feed(second, 1, entry(0, 10, 7));
        feed(second, 0, entry(0, 30, 7));
        assertEquals(first.getRecoveredEntries(), second.getRecoveredEntries());
        assertEquals(10, first.getRecoveredEntries().get(0).getValue());
    }

    public void testDuplicatePositionWithinPromiseCannotCountAsTwoAcceptors() {
        var processor = processor(3, 3);
        feed(processor, 0, entry(0, 20, 7), entry(0, 20, 7));
        feed(processor, 1, entry(0, 10, 7));
        feed(processor, 2);
        try {
            processor.getRecoveredEntries();
            fail("Malformed promise must not create duplicate votes");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Duplicate instance"));
        }
    }

    public void testRecoveryQuorumIsIndependentFromFastDecisionQuorum() {
        // N=7, recovery=4, Fast=6: three replies in Q preserve a possible decision.
        var processor = new FastPhaseOneResponseProcessor(10, List.of(0, 1, 2, 3, 4, 5, 6), 4, 6);
        feed(processor, 0, entry(0, 20, 7));
        feed(processor, 1, entry(0, 20, 7));
        assertFalse(feed(processor, 2, entry(0, 20, 7)));
        assertTrue(feed(processor, 3, entry(0, 10, 7)));
        assertEquals(20, processor.getRecoveredEntries().get(0).getValue());
    }

    public void testEveryFiveAcceptorVoteAssignmentPreservesAnyPossibleDecision() {
        // All 3^5 assignments of {no vote, X, Y}, with every possible four-node recovery quorum.
        for (int assignment = 0; assignment < 243; assignment++) {
            int[] values = new int[5];
            int remaining = assignment;
            int xVotes = 0;
            int yVotes = 0;
            for (int id = 0; id < 5; id++) {
                int vote = remaining % 3;
                remaining /= 3;
                if (vote == 1) { values[id] = 20; xVotes++; }
                if (vote == 2) { values[id] = 10; yVotes++; }
            }
            for (int omitted = 0; omitted < 5; omitted++) {
                FastPhaseOneResponseProcessor processor = processor(5, 4);
                boolean hasCandidate = false;
                for (int id = 0; id < 5; id++) {
                    if (id == omitted) continue;
                    if (values[id] == 0) feed(processor, id);
                    else {
                        feed(processor, id, entry(0, values[id], 6));
                        hasCandidate = true;
                    }
                }
                Map<Integer, DidaTradePaxos.AcceptedEntry> recovered = processor.getRecoveredEntries();
                assertEquals(hasCandidate, recovered.containsKey(0));
                if (xVotes >= 4) assertEquals(20, recovered.get(0).getValue());
                if (yVotes >= 4) assertEquals(10, recovered.get(0).getValue());
            }
        }
    }

    public void testUnsafeRecoveryQuorumIsRejected() {
        try {
            new FastPhaseOneResponseProcessor(10, List.of(0, 1, 2, 3, 4), 2, 4);
            fail("Recovery 2 cannot safely recover independent Fast votes with quorum 4 of 5");
        } catch (IllegalArgumentException expected) {
            // Recovery quorum must intersect the overlap of any two Fast quorums.
        }
    }
}
