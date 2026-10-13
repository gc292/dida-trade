package didatrade.server;

import junit.framework.TestCase;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RequestHistoryTest extends TestCase {
    public void testPendingSelectionFollowsArrivalRatherThanRequestId() {
        RequestHistory history = new RequestHistory();
        RequestRecord first = record(900);
        RequestRecord second = record(100);
        history.addToPending(900, first);
        history.addToPending(100, second);
        history.addToPending(500, record(500));
        assertSame(first, history.getFirstPending());
        assertSame(second, history.getFirstPendingExcluding(java.util.Set.of(900)));
        assertNull(history.getFirstPendingExcluding(java.util.Set.of(900, 100, 500)));
        history.moveToProcessed(900);
        assertSame(second, history.getFirstPending());
    }

    public void testExcludedLoserAndRetransmissionKeepOriginalArrivalPriority() {
        RequestHistory history = new RequestHistory();
        RequestRecord loser = record(900);
        RequestRecord later = record(100);
        history.addToPending(900, loser);
        history.addToPending(100, later);
        assertSame(later, history.getFirstPendingExcluding(java.util.Set.of(900)));
        for (int id = 1000; id < 1100; id++) history.addToPending(id, record(id));
        assertSame(loser, history.addToPending(900, record(900)));
        assertSame(loser, history.getFirstPendingExcluding(java.util.Set.of()));
        history.moveToProcessed(900);
        assertSame(loser, history.addToPending(900, record(900)));
        assertSame(later, history.getFirstPending());
    }

    private RequestRecord record(int id) {
        return new RequestRecord(id, new DidaTradeCommand(DidaTradeAction.BUY, 1, 50));
    }

    public void testPendingRetransmissionKeepsOriginalWaiter() {
        RequestHistory history = new RequestHistory();
        RequestRecord first = record(101);
        history.addToPending(101, first);
        history.addToPending(101, record(101));
        assertSame(first, history.getIfPending(101));
    }

    public void testProcessedRequestCannotBecomePendingAgain() {
        RequestHistory history = new RequestHistory();
        RequestRecord first = record(101);
        history.addToPending(101, first);
        first.setResponse(true);
        history.moveToProcessed(101);
        history.addToPending(101, record(101));
        assertNull(history.getIfPending(101));
        assertSame(first, history.getIfExists(101));
        assertTrue(first.waitForResponse());
    }

    public void testConcurrentRegistrationsUseOneRecord() throws Exception {
        RequestHistory history = new RequestHistory();
        var pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        ArrayList<Future<RequestRecord>> results = new ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    history.addToPending(101, record(101));
                    return history.getIfExists(101);
                }));
            }
            start.countDown();
            RequestRecord canonical = results.get(0).get(3, TimeUnit.SECONDS);
            for (Future<RequestRecord> result : results) {
                assertSame(canonical, result.get(3, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    public void testBalanceResultDoesNotChangeRequestIdentity() {
        RequestHistory history = new RequestHistory();
        RequestRecord first = new RequestRecord(101, new DidaTradeCommand(DidaTradeAction.BALANCE, 1));
        history.addToPending(101, first);
        first.getRequest().setQuantity(17);
        first.setResponse(true);
        history.moveToProcessed(101);
        assertSame(first, history.addToPending(101,
                new RequestRecord(101, new DidaTradeCommand(DidaTradeAction.BALANCE, 1))));
        assertEquals(17, first.getRequest().getQuantity());
    }

    public void testDifferentPayloadCannotReplaceCanonicalRecord() {
        RequestHistory history = new RequestHistory();
        RequestRecord first = record(101);
        history.addToPending(101, first);
        try {
            history.addToPending(101,
                    new RequestRecord(101, new DidaTradeCommand(DidaTradeAction.BUY, 1, 25)));
            fail("Different payload must be rejected");
        } catch (IllegalArgumentException expected) {
            assertSame(first, history.getIfPending(101));
        }
    }
}
