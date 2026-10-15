package didatrade.server;

import didatrade.configs.ConfigurationScheduler;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.DidaTradePaxos;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class SchedulerQuorumTest extends TestCase {
    public void testScheduleAConventionalMultiRemainsTwoOfThree() {
        ConfigurationScheduler scheduler = new ConfigurationScheduler('A');
        for (int ballot = 0; ballot < 6; ballot++) {
            assertEquals(3, scheduler.acceptors(ballot).size());
            assertEquals(2, scheduler.quorum(ballot).intValue());
            assertEquals(2, scheduler.recoveryQuorum(ballot).intValue());
            assertEquals(3, scheduler.superMajority(ballot).intValue());
            assertEquals(ballot % 3, scheduler.leader(ballot).intValue());
        }
    }

    public void testScheduleBFiveAcceptorsAndDistinctQuorumPolicies() {
        ConfigurationScheduler scheduler = new ConfigurationScheduler('B');
        for (int ballot = 0; ballot < 6; ballot++) {
            assertEquals(List.of(0, 1, 2, 3, 4), scheduler.acceptors(ballot));
            assertEquals(scheduler.acceptors(ballot), scheduler.learners(ballot));
            assertEquals(scheduler.acceptors(ballot), scheduler.allparticipants());
            assertEquals(3, scheduler.quorum(ballot).intValue());
            assertEquals(4, scheduler.superMajority(ballot).intValue());
            assertEquals(4, scheduler.recoveryQuorum(ballot).intValue());
            assertTrue(scheduler.fastpaxos(ballot));
            assertEquals(ballot % 3, scheduler.leader(ballot).intValue());
        }
    }

    public void testScheduleCExposesQuorumsWithoutClaimingStateTransferSupport() {
        ConfigurationScheduler scheduler = new ConfigurationScheduler('C');
        assertEquals(7, scheduler.allparticipants().size());
        assertEquals(List.of(0, 1, 2, 3, 4), scheduler.acceptors(0));
        assertEquals(List.of(2, 3, 4, 5, 6), scheduler.acceptors(2));
        for (int ballot = 0; ballot < 5; ballot++) {
            assertEquals(3, scheduler.quorum(ballot).intValue());
            assertEquals(4, scheduler.superMajority(ballot).intValue());
            assertEquals(3, scheduler.recoveryQuorum(ballot).intValue());
        }
    }

    public void testQuorumIntersectionsWithinEachConfiguration() {
        for (char option : new char[] {'A', 'B', 'C'}) {
            ConfigurationScheduler scheduler = new ConfigurationScheduler(option);
            for (int ballot = 0; ballot < 5; ballot++) {
                int n = scheduler.acceptors(ballot).size();
                int classic = scheduler.quorum(ballot);
                int recovery = scheduler.recoveryQuorum(ballot);
                int fast = scheduler.superMajority(ballot);
                assertTrue(2 * classic > n);
                assertTrue(recovery + classic > n);
                assertTrue(2 * fast > n);
                assertTrue(recovery + 2 * fast > 2 * n);
            }
        }
        // These intersections assume one acceptor set; they do not validate C state transfer.
    }

    public void testConventionalPhaseOneUsesRecoveryQuorumRatherThanDecisionQuorum() {
        ConfigurationScheduler scheduler = new ConfigurationScheduler('B');
        PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor(scheduler, 0, 1);
        for (int id = 0; id < 4; id++) {
            boolean done = processor.onNext(new ArrayList<>(), DidaTradePaxos.PhaseOneReply.newBuilder()
                    .setServerid(id).setRequestballot(1).setAccepted(true).setMaxballot(1).build());
            assertEquals(id == 3, done);
            assertEquals(id == 3, processor.getAccepted());
        }
    }
}
