package didatrade.server;

import didatrade.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import junit.framework.TestCase;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/** Five real Schedule B processes, using the production Fast loop and gRPC services. */
public class FastConsensusRegressionTest extends TestCase {
    public void testFastAgreementDeduplicationAndMatchingExecution() throws Exception {
        try (Cluster c = new Cluster("agreement")) {
            c.addEverywhere(101, 42, 20);
            c.addEverywhere(101, 42, 20);
            c.addEverywhere(201, 43, 30);
            List<Future<Integer>> balances = new ArrayList<>();
            for (int id = 0; id < 5; id++) {
                final int replica = id;
                balances.add(c.pool.submit(() -> c.main(replica).balance(DidaTradeMain.BalanceRequest
                        .newBuilder().setReqid(301).setUserid(42).build()).getBalance()));
            }
            for (Future<Integer> balance : balances) assertEquals(20, balance.get(20, TimeUnit.SECONDS).intValue());
            c.checkPrefixes();
            for (Path log : c.logs) {
                assertEquals(1, c.executions(log).stream().filter(line -> line.contains("id = 101 ")).count());
            }
        }
    }

    public void testOneFrozenAcceptorProgressesButTwoDoNotCauseCollision() throws Exception {
        try (Cluster c = new Cluster("quorums")) {
            c.awaitWindow();
            c.debug(4, DebugMode.FREEZE);
            c.addToReplicas(101, 42, 20, 4);
            assertEquals(0, c.ballot(0));
            c.debug(3, DebugMode.FREEZE);
            List<Future<Boolean>> pending = c.adds(201, 43, 30, 3);
            Thread.sleep(2800); // Greater than an RPC deadline; silence must not advance the ballot.
            for (Future<Boolean> reply : pending) assertFalse(reply.isDone());
            for (int id = 0; id < 3; id++) assertEquals(0, c.ballot(id));
            c.debug(3, DebugMode.UNFREEZE);
            Future<Boolean> fourth = c.pool.submit(() -> c.add(3, 201, 43, 30));
            for (Future<Boolean> reply : pending) assertTrue(reply.get(20, TimeUnit.SECONDS));
            assertTrue(fourth.get(20, TimeUnit.SECONDS));
            c.debug(4, DebugMode.UNFREEZE);
            c.addEverywhere(101, 42, 20);
            c.addEverywhere(201, 43, 30);
            c.checkPrefixes();
        }
    }

    public void testCollisionAutomaticallyRecoversAndOldNotificationsCannotAdvanceAgain() throws Exception {
        try (Cluster c = new Cluster("collision")) {
            c.awaitWindow();
            // Genuine acceptor votes: 2 versus 2 proves that no fourth matching vote is possible.
            for (int acceptor = 0; acceptor < 4; acceptor++) {
                assertTrue(c.paxos(acceptor).phasetwo(DidaTradePaxos.PhaseTwoRequest.newBuilder()
                        .setInstance(0).setRequestballot(0).setValue(acceptor < 2 ? 101 : 201)
                        .build()).getAccepted());
            }
            c.await(() -> {
                for (int id = 0; id < 5; id++) if (c.ballot(id) != 1) return false;
                return c.master(1).newballot(DidaTradeMaster.NewBallotRequest.newBuilder()
                        .setNewballot(1).setCompletedballot(-1).build()).getCompletedballot() == 1;
            });
            // Replay the real old votes, including duplicate collision signals at all learners.
            for (int learner = 0; learner < 5; learner++) {
                for (int acceptor = 0; acceptor < 4; acceptor++) {
                    c.paxos(learner).learn(DidaTradePaxos.LearnRequest.newBuilder().setInstance(0)
                            .setBallot(0).setServerid(acceptor).setValue(acceptor < 2 ? 101 : 201).build());
                }
                assertEquals(1, c.ballot(learner));
            }
            c.addEverywhere(101, 42, 20); // Winner payload arrived after the recovery decision.
            c.addEverywhere(201, 43, 30); // Losing command remains eligible.
            c.checkPrefixes();

            c.debug(0, DebugMode.FREEZE);
            // Fast acknowledgement does not wait for a command to be decided in ballot 2.
            assertTrue(c.master(2).newballot(DidaTradeMaster.NewBallotRequest.newBuilder()
                    .setNewballot(2).setCompletedballot(-1).build()).getCompletedballot() < 2);
            c.await(() -> c.ballot(1) == 2 && c.ballot(2) == 2 && c.ballot(3) == 2 && c.ballot(4) == 2);
            List<Future<Boolean>> replies = new ArrayList<>();
            for (int id = 1; id < 5; id++) {
                final int replica = id;
                replies.add(c.pool.submit(() -> c.add(replica, 301, 44, 40)));
            }
            for (Future<Boolean> reply : replies) assertTrue(reply.get(20, TimeUnit.SECONDS));
            c.debug(0, DebugMode.UNFREEZE);
            c.addEverywhere(301, 44, 40);
            c.checkPrefixes();
        }
    }

    public void testConcurrentRequestsSlowModeAndIdenticalState() throws Exception {
        try (Cluster c = new Cluster("concurrent")) {
            c.debug(4, DebugMode.SLOW_ON);
            List<Future<Boolean>> replies = new ArrayList<>();
            for (int user = 50; user < 54; user++) replies.addAll(c.adds(1000 + user, user, 40, 5));
            for (Future<Boolean> reply : replies) assertTrue(reply.get(20, TimeUnit.SECONDS));
            c.debug(4, DebugMode.SLOW_OFF);
            // Same payload with a distinct ID must execute again and observe the existing user.
            for (Future<Boolean> reply : c.adds(2000, 50, 40, 5)) assertFalse(reply.get(20, TimeUnit.SECONDS));
            List<Future<Boolean>> dumps = new ArrayList<>();
            for (int id = 0; id < 5; id++) {
                final int replica = id;
                dumps.add(c.pool.submit(() -> c.main(replica).dump(DidaTradeMain.DumpRequest
                        .newBuilder().setReqid(3000).build()).getResult()));
            }
            for (Future<Boolean> reply : dumps) assertTrue(reply.get(20, TimeUnit.SECONDS));
            c.checkPrefixes();
            String expected = c.dump(c.logs.get(0));
            for (int id = 1; id < 5; id++) assertEquals(expected, c.dump(c.logs.get(id)));
        }
    }

    public void testOutOfOrderDecisionsWaitForPrefixAndLatePayload() throws Exception {
        try (Cluster c = new Cluster("ordering")) {
            c.awaitWindow();
            for (int id = 0; id < 5; id++) assertTrue(c.paxos(id).phasetwo(DidaTradePaxos.PhaseTwoRequest
                    .newBuilder().setInstance(1).setRequestballot(0).setValue(201).build()).getAccepted());
            List<Future<Boolean>> later = c.adds(201, 43, 30, 5);
            Thread.sleep(200);
            for (Future<Boolean> reply : later) assertFalse(reply.isDone());
            for (int id = 0; id < 5; id++) assertTrue(c.paxos(id).phasetwo(DidaTradePaxos.PhaseTwoRequest
                    .newBuilder().setInstance(0).setRequestballot(0).setValue(101).build()).getAccepted());
            Thread.sleep(200);
            for (Future<Boolean> reply : later) assertFalse(reply.isDone());
            c.addEverywhere(101, 42, 20);
            for (Future<Boolean> reply : later) assertTrue(reply.get(20, TimeUnit.SECONDS));
            c.checkPrefixes();
            List<String> executed = c.executions(c.logs.get(0));
            assertTrue(executed.get(0).contains("id = 101 "));
            assertTrue(executed.get(1).contains("id = 201 "));
        }
    }

    public void testFastRejectsSchedulesWithoutTheStepThreeConfiguration() {
        for (char schedule : new char[] {'A', 'C'}) {
            try {
                new DidaTradeServerState(19000, 0, schedule, ProtocolMode.FAST);
                fail("Fast mode must reject Schedule " + schedule);
            } catch (IllegalArgumentException expected) {
                assertEquals("Fast Paxos requires Schedule B", expected.getMessage());
            }
        }
    }

    public void testDumpComparisonIgnoresProtocolNoiseButRetainsStateDifferences() {
        String clean = " ----------- Users ----------- \n"
                + "\t user 42 balance = 10 stock = 20\n"
                + " ----------- Sell Orders ----------- \n"
                + "\t order from user 42 with 4 available\n"
                + " ----------- Buy Orders ----------- \n"
                + "\t order from user 43 with 3 pending\n"
                + " -----------    done     ----------- \n";
        String noisy = clean.replace("\t user", "Paxos acceptor: going to notify learners\n"
                + "instance: 3\nballot: 1\nvalue: 1050\nserverid: 4\n\t user");
        assertEquals(stateDump(clean), stateDump(noisy));
        assertFalse(stateDump(clean).equals(stateDump(noisy.replace("balance = 10", "balance = 11"))));
        assertFalse(stateDump(clean).equals(stateDump(noisy.replace("4 available", "5 available"))));
        assertFalse(stateDump(clean).equals(stateDump(noisy.replace("3 pending", "2 pending"))));
    }

    private static String stateDump(String contents) {
        int begin = contents.lastIndexOf(" ----------- Users ----------- ");
        assertTrue(begin >= 0);
        int end = contents.indexOf(" -----------    done     ----------- ", begin);
        assertTrue(end > begin);
        StringBuilder state = new StringBuilder();
        for (String line : contents.substring(begin, end).split("\n")) {
            String record = line.trim();
            // RPC threads also print here; compare all state records, not their interleaved logs.
            if (record.startsWith("-----------") || record.startsWith("user ") || record.startsWith("order ")) {
                state.append(record).append('\n');
            }
        }
        return state.toString();
    }

    private static final class Cluster implements AutoCloseable {
        final List<Process> processes = new ArrayList<>();
        final List<ManagedChannel> channels = new ArrayList<>();
        final List<Path> logs = new ArrayList<>();
        final ExecutorService pool = Executors.newFixedThreadPool(25);

        Cluster(String scenario) throws Exception {
            int base = freePorts();
            Path directory = Path.of("target", "fast-consensus", scenario);
            Files.createDirectories(directory);
            String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            try {
                for (int id = 0; id < 5; id++) {
                    Path log = directory.resolve("s" + id + ".log");
                    logs.add(log);
                    processes.add(new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                            "-cp", cp, DidaTradeServer.class.getName(), Integer.toString(base), Integer.toString(id), "B", "fast")
                            .redirectErrorStream(true).redirectOutput(log.toFile()).start());
                    channels.add(ManagedChannelBuilder.forAddress("localhost", base + id).usePlaintext().build());
                }
                for (int id = 0; id < 5; id++) {
                    final int port = base + id;
                    await(() -> {
                        try (Socket ignored = new Socket("localhost", port)) { return true; }
                        catch (java.io.IOException error) { return false; }
                    });
                }
            } catch (Exception | AssertionError error) {
                close();
                throw error;
            }
        }

        static int freePorts() throws Exception {
            for (int attempt = 0; attempt < 100; attempt++) {
                List<ServerSocket> sockets = new ArrayList<>();
                try {
                    ServerSocket first = new ServerSocket(0);
                    sockets.add(first);
                    int base = first.getLocalPort();
                    if (base > 65531) continue;
                    for (int offset = 1; offset < 5; offset++) sockets.add(new ServerSocket(base + offset));
                    return base;
                } catch (java.io.IOException occupied) {
                    // Try another contiguous range.
                } finally {
                    for (ServerSocket socket : sockets) socket.close();
                }
            }
            throw new IllegalStateException("No free range for five replicas");
        }

        void await(Callable<Boolean> condition) throws Exception {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < end) {
                for (Process process : processes) assertTrue("Replica exited; inspect " + logs, process.isAlive());
                if (condition.call()) return;
                Thread.sleep(50);
            }
            fail("Timed out; inspect " + logs);
        }

        void awaitWindow() throws Exception {
            await(() -> {
                for (int id = 0; id < 5; id++) {
                    // Probe idempotent ANY for the last slot. It also ensures authorization before directed votes.
                    if (!paxos(id).phasetwo(DidaTradePaxos.PhaseTwoRequest.newBuilder()
                            .setInstance(3).setRequestballot(0).setAny(true).build()).getAccepted()) return false;
                }
                return true;
            });
            // Authorize the directed-vote slot idempotently, avoiding a race with startup RPC delivery.
            for (int id = 0; id < 5; id++) assertTrue(paxos(id).phasetwo(DidaTradePaxos.PhaseTwoRequest
                    .newBuilder().setInstance(0).setRequestballot(0).setAny(true).build()).getAccepted());
        }

        DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub main(int id) {
            return DidaTradeMainServiceGrpc.newBlockingStub(channels.get(id)).withDeadlineAfter(20, TimeUnit.SECONDS);
        }
        DidaTradePaxosServiceGrpc.DidaTradePaxosServiceBlockingStub paxos(int id) {
            return DidaTradePaxosServiceGrpc.newBlockingStub(channels.get(id)).withDeadlineAfter(3, TimeUnit.SECONDS);
        }
        DidaTradeMasterServiceGrpc.DidaTradeMasterServiceBlockingStub master(int id) {
            return DidaTradeMasterServiceGrpc.newBlockingStub(channels.get(id)).withDeadlineAfter(3, TimeUnit.SECONDS);
        }
        int ballot(int id) {
            return paxos(id).phaseone(DidaTradePaxos.PhaseOneRequest.newBuilder().setRequestballot(0).build()).getMaxballot();
        }
        void debug(int id, DebugMode mode) {
            master(id).setdebug(DidaTradeMaster.SetDebugRequest.newBuilder().setMode(mode.getCode()).build());
        }
        boolean add(int replica, int id, int user, int cash) {
            return main(replica).adduser(DidaTradeMain.AddUserRequest.newBuilder()
                    .setReqid(id).setUserid(user).setCash(cash).setStock(20).build()).getResult();
        }
        List<Future<Boolean>> adds(int id, int user, int cash, int count) {
            List<Future<Boolean>> replies = new ArrayList<>();
            for (int replica = 0; replica < count; replica++) {
                final int target = replica;
                replies.add(pool.submit(() -> add(target, id, user, cash)));
            }
            return replies;
        }
        void addToReplicas(int id, int user, int cash, int count) throws Exception {
            for (Future<Boolean> reply : adds(id, user, cash, count)) assertTrue(reply.get(20, TimeUnit.SECONDS));
        }
        void addEverywhere(int id, int user, int cash) throws Exception { addToReplicas(id, user, cash, 5); }
        List<String> executions(Path log) throws Exception {
            return Files.readAllLines(log).stream().filter(line -> line.startsWith("Setting response for command with id = ")).toList();
        }
        String dump(Path log) throws Exception {
            return stateDump(Files.readString(log));
        }
        void checkPrefixes() throws Exception {
            List<String> expected = executions(logs.get(0));
            assertFalse(expected.isEmpty());
            for (int id = 1; id < 5; id++) assertEquals(expected, executions(logs.get(id)));
        }
        @Override public void close() throws Exception {
            pool.shutdownNow();
            for (ManagedChannel channel : channels) channel.shutdownNow();
            for (Process process : processes) process.destroy();
            for (Process process : processes) {
                if (!process.waitFor(3, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    assertTrue(process.waitFor(3, TimeUnit.SECONDS));
                }
            }
        }
    }
}
