package didatrade.server;

/**
 * Step 3 coordinator scaffold, deliberately not wired into server startup.
 * Implementation order: Design-decisions/Step-3-Implementation-Guide.md.
 * Protocol TODOs fail explicitly instead of pretending consensus succeeded.
 */
public final class FastPaxosMainLoop implements ConsensusLoop {
    private final DidaTradeServerState serverState;

    public FastPaxosMainLoop(DidaTradeServerState serverState) {
        this.serverState = serverState;
    }

    @Override
    public void run() {
        // TODO FAST-08: event loop; every replica executes/votes, only the leader prepares/opens.
        throw new UnsupportedOperationException("TODO FAST-08: Fast coordinator event loop");
    }

    @Override
    public synchronized void wakeup() {
        // TODO FAST-08: publish work and notify the waiting loop, without network calls here.
        throw new UnsupportedOperationException("TODO FAST-08: Fast coordinator wakeup");
    }

    boolean prepareLeadership(int ballot) {
        // TODO FAST-04: Phase 1 once per ballot; recover all positions before opening ANY.
        throw new UnsupportedOperationException("TODO FAST-04: prepare Fast leadership");
    }

    void openNextInstances(int ballot) {
        // TODO FAST-06: pre-open a bounded consecutive window; begin with one position.
        throw new UnsupportedOperationException("TODO FAST-06: open Fast instances");
    }

    void proposePendingRequests(int ballot) {
        // TODO FAST-06: local acceptor votes only in authorized positions, from pending payloads.
        throw new UnsupportedOperationException("TODO FAST-06: local Fast proposals");
    }

    void startRecoveredProposals(int ballot) {
        // TODO FAST-04: retain the selected concrete value for each (ballot, instance).
        throw new UnsupportedOperationException("TODO FAST-04: recover accepted positions");
    }

    void processCompletedAttempts() {
        // TODO FAST-04/06: distinguish ANY authorization from concrete-value consensus.
        // Old successes remain decisions; old failures cannot retry under old ballots.
        throw new UnsupportedOperationException("TODO FAST-04/06: consume Fast attempts");
    }

    void executeDecidedPrefix() {
        // TODO FAST-07: consume processed IDs; wait for missing payload; never reset the cursor.
        throw new UnsupportedOperationException("TODO FAST-07: ordered Fast execution");
    }

    public void onCollision(int instance, int ballot) {
        // TODO FAST-09: publish a collision event; transition only once from current b to b+1.
        throw new UnsupportedOperationException("TODO FAST-09: collision handling");
    }
}
