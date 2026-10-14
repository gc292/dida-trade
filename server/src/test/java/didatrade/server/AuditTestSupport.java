package didatrade.server;

import didatrade.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import junit.framework.Assert;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;

/** Test-only transport harness; all state, services and coordinators are production classes. */
final class AuditTestSupport extends Assert {
    static String classpath() {
        return System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    }

    // Classic/Multi workers deliberately have permanent lifetimes. Isolate them rather
    // than adding shutdown mechanisms to production solely to accommodate these tests.
    static void isolated(Class<?> owner, String scenario) throws Exception {
        Path directory = Path.of("target", "audit-follow-up");
        Files.createDirectories(directory);
        Path log = directory.resolve(owner.getSimpleName() + "-" + scenario + ".log");
        Process child = new ProcessBuilder(java(), "-cp", classpath(), owner.getName(), scenario)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue("Scenario timed out: " + log, child.waitFor(40, TimeUnit.SECONDS));
            List<String> lines = Files.readAllLines(log);
            String tail = String.join("\n", lines.subList(Math.max(0, lines.size() - 30), lines.size()));
            assertEquals("Scenario " + scenario + "; evidence: " + log + "\n" + tail, 0, child.exitValue());
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    static String java() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    static boolean eventually(long millis, BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        do {
            if (condition.getAsBoolean()) return true;
            Thread.sleep(10);
        } while (System.nanoTime() < end);
        return condition.getAsBoolean();
    }

    static void await(String description, BooleanSupplier condition) throws Exception {
        assertTrue("Timed out: " + description, eventually(8000, condition));
    }

    static final class Probe {
        final List<DidaTradeMain.BalanceRequest> reads = new CopyOnWriteArrayList<>();
        final AtomicInteger readsEntered = new AtomicInteger();
        final AtomicInteger readsReturned = new AtomicInteger();
        final AtomicInteger paxosCalls = new AtomicInteger();
    }

    static final class Cluster implements AutoCloseable {
        final int base;
        final DidaTradeServerState[] states;
        final Probe[] probes;
        final Server[] servers;
        final ManagedChannel[] channels;
        final ExecutorService pool = Executors.newCachedThreadPool();
        private final IntFunction<ServerInterceptor> interceptors;

        Cluster(ProtocolMode mode, boolean workers) throws Exception {
            this(mode, workers, ignored -> null);
        }

        Cluster(ProtocolMode mode, boolean workers, IntFunction<ServerInterceptor> interceptors) throws Exception {
            int n = mode == ProtocolMode.FAST ? 5 : 3;
            base = ports(n);
            this.interceptors = interceptors;
            states = new DidaTradeServerState[n];
            probes = new Probe[n];
            servers = new Server[n];
            channels = new ManagedChannel[n];
            try {
                for (int id = 0; id < n; id++) {
                    probes[id] = new Probe();
                    states[id] = new DidaTradeServerState(base, id, mode == ProtocolMode.FAST ? 'B' : 'A', mode, workers);
                    if (workers) states[id].main_loop_worker.setName("audit-" + mode + "-" + id);
                    startTransport(id);
                    channels[id] = ManagedChannelBuilder.forAddress("localhost", base + id).usePlaintext().build();
                }
            } catch (Throwable error) {
                close();
                throw error;
            }
        }

        void startTransport(int id) throws Exception {
            Probe probe = probes[id];
            DidaTradeMainServiceImpl main = new DidaTradeMainServiceImpl(states[id]) {
                @Override public void balance(DidaTradeMain.BalanceRequest request,
                                               StreamObserver<DidaTradeMain.BalanceReply> observer) {
                    probe.reads.add(request);
                    probe.readsEntered.incrementAndGet();
                    try { super.balance(request, observer); }
                    finally { probe.readsReturned.incrementAndGet(); }
                }
            };
            ServerInterceptor count = new ServerInterceptor() {
                @Override public <Req, Resp> ServerCall.Listener<Req> interceptCall(
                        ServerCall<Req, Resp> call, Metadata headers, ServerCallHandler<Req, Resp> next) {
                    if (call.getMethodDescriptor().getFullMethodName().contains("PaxosService/"))
                        probe.paxosCalls.incrementAndGet();
                    return next.startCall(call, headers);
                }
            };
            ServerBuilder<?> builder = ServerBuilder.forPort(base + id)
                    .addService(main).addService(new DidaTradeMasterServiceImpl(states[id]))
                    .addService(new DidaTradePaxosServiceImpl(states[id])).intercept(count);
            ServerInterceptor interceptor = interceptors.apply(id);
            if (interceptor != null) builder.intercept(interceptor);
            servers[id] = builder.build().start();
        }

        DidaTradeMainServiceGrpc.DidaTradeMainServiceBlockingStub main(int id) {
            return DidaTradeMainServiceGrpc.newBlockingStub(channels[id]).withDeadlineAfter(8, TimeUnit.SECONDS);
        }

        DidaTradePaxosServiceGrpc.DidaTradePaxosServiceBlockingStub paxos(int id) {
            return DidaTradePaxosServiceGrpc.newBlockingStub(channels[id]).withDeadlineAfter(3, TimeUnit.SECONDS);
        }

        DidaTradeMain.AddUserReply add(int replica, int reqid, int cash) {
            return main(replica).adduser(DidaTradeMain.AddUserRequest.newBuilder()
                    .setReqid(reqid).setUserid(42).setCash(cash).setStock(20).build());
        }

        DidaTradeMain.BalanceReply read(int replica, int reqid, long minVersion) {
            return main(replica).balance(readRequest(reqid, minVersion));
        }

        List<Future<DidaTradeMain.AddUserReply>> addAll(int reqid, int cash) {
            List<Future<DidaTradeMain.AddUserReply>> replies = new ArrayList<>();
            for (int id = 0; id < states.length; id++) {
                final int replica = id;
                replies.add(pool.submit(() -> add(replica, reqid, cash)));
            }
            return replies;
        }

        void buyAll(int reqid, int fraction) throws Exception {
            List<Future<DidaTradeMain.BuyReply>> replies = new ArrayList<>();
            for (int id = 0; id < states.length; id++) {
                final int replica = id;
                replies.add(pool.submit(() -> main(replica).buy(DidaTradeMain.BuyRequest.newBuilder()
                        .setReqid(reqid).setUserid(42).setQuantity(fraction).build())));
            }
            for (Future<DidaTradeMain.BuyReply> reply : replies) assertTrue(reply.get(8, TimeUnit.SECONDS).getResult());
        }

        @Override public void close() {
            pool.shutdownNow();
            for (ManagedChannel channel : channels) if (channel != null) channel.shutdownNow();
            for (Server server : servers) if (server != null) server.shutdownNow();
            for (DidaTradeServerState state : states) if (state != null) {
                for (ManagedChannel channel : state.channels) channel.shutdownNow();
                if (state.main_loop_worker != null) state.main_loop_worker.interrupt();
            }
        }
    }

    static DidaTradeMain.BalanceRequest readRequest(int reqid, long minVersion) {
        return DidaTradeMain.BalanceRequest.newBuilder().setReqid(reqid).setUserid(42)
                .setMinVersion(minVersion).build();
    }

    private static int ports(int n) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            List<ServerSocket> reserved = new ArrayList<>();
            try {
                ServerSocket first = new ServerSocket(0);
                reserved.add(first);
                int base = first.getLocalPort();
                if (base + n > 65535) continue;
                for (int id = 1; id < n; id++) reserved.add(new ServerSocket(base + id));
                return base;
            } catch (java.io.IOException occupied) {
                // Try another contiguous range.
            } finally {
                for (ServerSocket socket : reserved) socket.close();
            }
        }
        throw new IllegalStateException("No free replica port range");
    }
}
