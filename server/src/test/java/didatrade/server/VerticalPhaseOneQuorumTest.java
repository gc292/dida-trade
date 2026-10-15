package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;
import didatrade.util.PhaseOneResponseProcessor;
import junit.framework.TestCase;

import java.util.ArrayList;

public class VerticalPhaseOneQuorumTest extends TestCase {
    private static boolean answer(PhaseOneResponseProcessor processor, int... servers) {
        boolean done = false;
        for (int server : servers) {
            DidaTradePaxos.PhaseOneReply reply = DidaTradePaxos.PhaseOneReply.newBuilder()
                    .setServerid(server).setAccepted(true).setValballot(-1).setMaxballot(2).build();
            done = processor.onNext(new ArrayList<>(), reply);
        }
        return done;
    }

    public void testNewAcceptorsAloneAreNotEnoughAfterTheChange() {
        ConfigurationScheduler    scheduler = new ConfigurationScheduler('C');
        PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor(scheduler, 0, 2);

        assertFalse(answer(processor, 3, 4, 5, 6));
        assertFalse(processor.getAccepted());
    }

    public void testQuorumOfBothConfigurationsSucceeds() {
        ConfigurationScheduler    scheduler = new ConfigurationScheduler('C');
        PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor(scheduler, 0, 2);

        assertTrue(answer(processor, 1, 2, 3, 4));
        assertTrue(processor.getAccepted());
    }

    public void testOldAcceptorsZeroAndOneMayBothBeGone() {
        ConfigurationScheduler    scheduler = new ConfigurationScheduler('C');
        PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor(scheduler, 0, 3);

        assertTrue(answer(processor, 2, 3, 4));
        assertTrue(processor.getAccepted());
    }

    public void testSingleConfigurationIsUnchanged() {
        ConfigurationScheduler    scheduler = new ConfigurationScheduler('A');
        PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor(scheduler, 0, 5);

        assertTrue(answer(processor, 0, 2));
        assertTrue(processor.getAccepted());
    }
}
