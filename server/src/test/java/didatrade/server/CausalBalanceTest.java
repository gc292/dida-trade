package didatrade.server;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import junit.framework.TestCase;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static didatrade.server.AuditTestSupport.*;

/** Step 4 tests over real gRPC services and the production state-machine executors. */
public class CausalBalanceTest extends TestCase {
    public void testBalanceBypassesPaxosInEveryProtocolMode() throws Exception { isolated(getClass(), "bypass"); }
    public void testWriteDependencyWaitsForLatePayloadAndResumes() throws Exception { isolated(getClass(), "write-read"); }
    public void testReadToReadDependencyAcrossReplicas() throws Exception { isolated(getClass(), "read-read"); }
    public void testBalanceAndVersionAreOneConcurrentStateSnapshot() throws Exception { isolated(getClass(), "snapshot"); }
    public void testDuplicateLogExecutionsDoNotAdvanceCausalVersion() throws Exception { isolated(getClass(), "duplicates"); }
    public void testCancellationEndsDependencyWaitWithoutBlockingWrites() throws Exception { isolated(getClass(), "cancel"); }
    public void testProductionClientPropagatesWriteAndReadVersions() throws Exception { isolated(getClass(), "client"); }

    public static void main(String[] args) {
        try {
            switch (args[0]) {
                case "bypass" -> bypass();
                case "write-read" -> dependency(false);
                case "read-read" -> dependency(true);
                case "snapshot" -> snapshot();
                case "duplicates" -> duplicates();
                case "cancel" -> cancellation();
                case "client" -> client();
                default -> throw new IllegalArgumentException(args[0]);
            }
            System.exit(0);
        } catch (Throwable error) {
            error.printStackTrace();
            System.exit(1);
        }
    }

    private static void bypass() throws Exception {
        for (ProtocolMode mode : ProtocolMode.values()) {
            // No worker can start consensus. Local reads must still be available.
            try (Cluster c = new Cluster(mode, false)) {
                for (int id = 0; id < c.states.length; id++) {
                    DidaTradeMain.BalanceReply reply = c.main(id).balance(DidaTradeMain.BalanceRequest
                            .newBuilder().setReqid(101).setUserid(0).build());
                    assertTrue(reply.getResult());
                    assertEquals(10, reply.getBalance());
                    assertEquals(0L, reply.getAppliedVersion());
                    assertNull(c.states[id].req_history.getIfExists(101));
                    assertTrue(c.states[id].paxos_log.getAcceptedEntries().isEmpty());
                    assertTrue(c.states[id].paxos_log.getDecidedEntriesFrom(0).isEmpty());
                    assertEquals(0, c.probes[id].paxosCalls.get());
                }
            }
        }
    }

    private static void dependency(boolean observedRead) throws Exception {
        try (Cluster c = new Cluster(ProtocolMode.MULTI, true)) {
            Future<DidaTradeMain.AddUserReply> first = c.pool.submit(() -> c.add(0, 101, 40));
            Future<DidaTradeMain.AddUserReply> second = c.pool.submit(() -> c.add(1, 101, 40));
            DidaTradeMain.AddUserReply write = first.get(8, TimeUnit.SECONDS);
            assertTrue(write.getResult());
            assertTrue(second.get(8, TimeUnit.SECONDS).getResult());
            await("follower knows decision but has no payload", () -> c.states[2].paxos_log.isDecided(0));
            assertEquals(0L, c.states[2].getAppliedVersion());
            long dependency = observedRead ? c.read(0, 201, 0).getAppliedVersion() : write.getAppliedVersion();
            assertEquals(1L, dependency);
            Future<DidaTradeMain.BalanceReply> read = c.pool.submit(() -> c.read(2, 301, dependency));
            await("causal read handler entered", () -> c.probes[2].readsEntered.get() == 1);
            Thread.sleep(150);
            assertFalse("Unfrozen lagging replica must wait for dependency", read.isDone());
            assertEquals(0, c.probes[2].readsReturned.get());
            assertTrue(c.add(2, 101, 40).getResult()); // Original payload, not another write.
            DidaTradeMain.BalanceReply reply = read.get(3, TimeUnit.SECONDS);
            assertTrue(reply.getResult());
            assertEquals(40, reply.getBalance());
            assertEquals(dependency, reply.getAppliedVersion());
            assertNull(c.states[2].req_history.getIfExists(301));
            assertEquals(1, c.states[2].paxos_log.getDecidedEntriesFrom(0).size());
        }
    }

    private static void snapshot() throws Exception {
        try (Cluster c = new Cluster(ProtocolMode.MULTI, true)) {
            for (Future<DidaTradeMain.AddUserReply> reply : c.addAll(101, 1000))
                assertTrue(reply.get(8, TimeUnit.SECONDS).getResult());
            Map<Long, Integer> expected = Map.of(1L, 1000, 2L, 500, 3L, 250, 4L, 125, 5L, 63, 6L, 32, 7L, 16);
            Future<?> writer = c.pool.submit(() -> {
                try {
                    for (int i = 0; i < 6; i++) {
                        c.buyAll(1001 + i, 50);
                        Thread.sleep(15);
                    }
                } catch (Exception error) { throw new RuntimeException(error); }
            });
            int samples = 0;
            do {
                for (int id = 0; id < c.states.length; id++) {
                    DidaTradeMain.BalanceReply reply = c.read(id, 2000 + samples++, 1);
                    assertTrue(reply.getResult());
                    assertEquals("Wallet and version must describe the same prefix",
                            expected.get(reply.getAppliedVersion()).intValue(), reply.getBalance());
                }
            } while (!writer.isDone() || samples < 60);
            writer.get(8, TimeUnit.SECONDS);
            for (int id = 0; id < c.states.length; id++) {
                DidaTradeMain.BalanceReply last = c.read(id, 3000, 7);
                assertEquals(7L, last.getAppliedVersion());
                assertEquals(16, last.getBalance());
            }
            System.out.println("AUD causal snapshot samples=" + samples);
        }
    }

    private static void duplicates() throws Exception {
        for (ProtocolMode mode : ProtocolMode.values()) {
            try (Cluster c = new Cluster(mode, true)) {
                for (Future<DidaTradeMain.AddUserReply> reply : c.addAll(101, 40))
                    assertTrue(reply.get(8, TimeUnit.SECONDS).getResult());
                int follower = c.states.length - 1;
                // Directed learner certificate tests executor deduplication, not
                // the reachability/agreement of an independently generated certificate.
                int quorum = mode == ProtocolMode.FAST ? 4 : 2;
                for (int acceptor = 0; acceptor < quorum; acceptor++) c.paxos(follower).learn(
                        DidaTradePaxos.LearnRequest.newBuilder().setInstance(1).setBallot(0)
                                .setValue(101).setServerid(acceptor).build());
                await("duplicate position consumed", () -> cursor(c.states[follower]) == 2);
                assertEquals(1L, c.states[follower].getAppliedVersion());
                DidaTradeMain.BalanceReply reply = c.read(follower, 201, 1);
                assertEquals(1L, reply.getAppliedVersion());
                assertEquals(40, reply.getBalance());
                assertTrue(c.add(follower, 101, 40).getResult());
                assertEquals(1L, c.states[follower].getAppliedVersion());
            }
        }
    }

    private static int cursor(DidaTradeServerState state) {
        try {
            String name = state.main_loop instanceof MainLoop ? "next_log_entry"
                    : state.main_loop instanceof MultiPaxosMainLoop ? "next_to_execute" : "nextToExecute";
            Field field = state.main_loop.getClass().getDeclaredField(name);
            field.setAccessible(true);
            synchronized (state.main_loop) { return field.getInt(state.main_loop); }
        } catch (ReflectiveOperationException error) { throw new AssertionError(error); }
    }

    private static void cancellation() throws Exception {
        try (Cluster c = new Cluster(ProtocolMode.MULTI, true)) {
            Future<?> read = c.pool.submit(() -> {
                try {
                    c.main(2).withDeadlineAfter(300, TimeUnit.MILLISECONDS).balance(readRequest(201, 1));
                    fail("Read should time out before any dependency exists");
                } catch (StatusRuntimeException expected) {
                    assertEquals(Status.Code.DEADLINE_EXCEEDED, expected.getStatus().getCode());
                }
            });
            await("waiting handler entered", () -> c.probes[2].readsEntered.get() == 1);
            assertEquals(0, c.probes[2].readsReturned.get());
            read.get(3, TimeUnit.SECONDS);
            await("cancelled production handler returned", () -> c.probes[2].readsReturned.get() == 1);
            assertEquals(0L, c.states[2].getAppliedVersion());
            for (Future<DidaTradeMain.AddUserReply> reply : c.addAll(101, 40))
                assertTrue(reply.get(8, TimeUnit.SECONDS).getResult());
            assertEquals(40, c.read(2, 301, 1).getBalance());
        }
    }

    private static void client() throws Exception {
        try (Cluster c = new Cluster(ProtocolMode.MULTI, true)) {
            Path appClasses = Path.of("..", "app", "target", "classes").toAbsolutePath().normalize();
            assertTrue("Build app before running the production-client test", Files.exists(appClasses.resolve("didatrade/app/DidaTradeApp.class")));
            Path output = Path.of("target", "audit-follow-up", "production-client.log");
            Process app = new ProcessBuilder(java(), "-cp", classpath() + java.io.File.pathSeparator + appClasses,
                    "didatrade.app.DidaTradeApp", "1", "localhost", Integer.toString(c.base), "A")
                    .redirectErrorStream(true).redirectOutput(output.toFile()).start();
            try (BufferedWriter input = new BufferedWriter(new OutputStreamWriter(app.getOutputStream()))) {
                send(input, "add 42 40 20");
                await("production app observed write reply", () -> contents(output).contains("user 42created."));
                await("all replicas executed app write", () -> java.util.Arrays.stream(c.states)
                        .allMatch(state -> state.getAppliedVersion() == 1));
                c.buyAll(902, 50); // Another client's write: app has not observed version 2 yet.
                send(input, "balance 42");
                await("first app read replied", () -> occurrences(contents(output), "User 42 balance is 20") == 1);
                send(input, "balance 42");
                await("second app read replied", () -> occurrences(contents(output), "User 42 balance is 20") == 2);
                send(input, "exit");
                assertTrue(app.waitFor(5, TimeUnit.SECONDS));
                assertEquals(contents(output), 0, app.exitValue());
                for (Probe probe : c.probes) {
                    await("both broadcasts arrived", () -> probe.reads.size() == 2);
                    List<DidaTradeMain.BalanceRequest> reads = probe.reads;
                    assertEquals(201, reads.get(0).getReqid());
                    assertEquals("Write reply propagated", 1L, reads.get(0).getMinVersion());
                    assertEquals(301, reads.get(1).getReqid());
                    assertEquals("Read observing another client's write propagated", 2L, reads.get(1).getMinVersion());
                }
                assertEquals(2L, c.states[0].getAppliedVersion());
            } finally {
                if (app.isAlive()) {
                    app.destroyForcibly();
                    app.waitFor(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static void send(BufferedWriter input, String command) throws Exception {
        input.write(command + "\n");
        input.flush();
    }

    private static String contents(Path path) {
        try { return Files.exists(path) ? Files.readString(path) : ""; }
        catch (java.io.IOException error) { throw new AssertionError(error); }
    }

    private static int occurrences(String text, String token) {
        return text.split(java.util.regex.Pattern.quote(token), -1).length - 1;
    }
}
