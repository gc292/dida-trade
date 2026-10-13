package didatrade.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class DidaTradeServer {

    static DidaTradeServerState server_state;

    /**
     * Server host port.
     */
    private static int port;

    public static void main(String[] args) throws Exception {

        System.out.println(DidaTradeServer.class.getSimpleName());

        // Print received arguments.
        System.out.printf("Received %d arguments%n", args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.printf("arg[%d] = %s%n", i, args[i]);
        }

        ProtocolMode protocol = ProtocolMode.CLASSIC;

        if (args.length >= 4) {
            switch (args[3].toLowerCase(java.util.Locale.ROOT)) {
                case "classic":
                    protocol = ProtocolMode.CLASSIC;
                    break;
                case "multi":
                    protocol = ProtocolMode.MULTI;
                    break;
                case "fast":
                    protocol = ProtocolMode.FAST;
                    break;
                default:
                    System.err.println("Invalid protocol: use classic, multi or fast.");
                    return;
            }
        }

        // Check arguments.
        if (args.length < 3) {
            System.err.println("Argument(s) missing!");
            System.err.printf("Usage: java %s baseport replica-id scheduler [classic|multi|fast]\n", DidaTradeServer.class.getName());
            return;
        }

        int  base_port = Integer.valueOf(args[0]);
        int  my_id     = Integer.valueOf(args[1]);
        char scheduler = args[2].charAt(0);

        if (protocol == ProtocolMode.FAST && scheduler != 'B') {
            System.err.println("Fast Paxos requires Schedule B.");
            return;
        }

        server_state = new DidaTradeServerState(base_port, my_id, scheduler, protocol);

        port = base_port + my_id;

        final BindableService service_impl = new DidaTradeMainServiceImpl(server_state);
        final BindableService master_impl  = new DidaTradeMasterServiceImpl(server_state);
        final BindableService paxos_impl   = new DidaTradePaxosServiceImpl(server_state);

        // Create a new server to listen on port.
        Server server = ServerBuilder.forPort(port).addService(service_impl).addService(master_impl).addService(paxos_impl).build();
        // Start the server.
        server.start();
        // Server threads are running in the background.
        System.out.println("Server started");

        // Do not exit the main thread. Wait until server is terminated.
        server.awaitTermination();
    }
}
