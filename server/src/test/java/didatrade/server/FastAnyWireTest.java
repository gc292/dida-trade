package didatrade.server;

import didatrade.DidaTradePaxos;
import junit.framework.TestCase;

public class FastAnyWireTest extends TestCase {
    public void testExistingConcreteRequestDefaultsToFalseWithoutChangingWireFields() throws Exception {
        DidaTradePaxos.PhaseTwoRequest request = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(2).setRequestballot(3).setValue(10).build();
        assertFalse(request.getAny());
        assertTrue(java.util.Arrays.equals(new byte[] {8, 2, 16, 3, 24, 10}, request.toByteArray()));
        assertFalse(DidaTradePaxos.PhaseTwoRequest.parseFrom(request.toByteArray()).getAny());
    }

    public void testAnyRoundTripsIndependentlyOfConcreteValue() throws Exception {
        DidaTradePaxos.PhaseTwoRequest request = DidaTradePaxos.PhaseTwoRequest.newBuilder()
                .setInstance(2).setRequestballot(3).setAny(true).build();
        DidaTradePaxos.PhaseTwoRequest parsed = DidaTradePaxos.PhaseTwoRequest.parseFrom(request.toByteArray());
        assertTrue(parsed.getAny());
        assertEquals(2, parsed.getInstance());
        assertEquals(3, parsed.getRequestballot());
        assertEquals(0, parsed.getValue());
        assertEquals(4, DidaTradePaxos.PhaseTwoRequest.getDescriptor().findFieldByName("any").getNumber());
    }
}
