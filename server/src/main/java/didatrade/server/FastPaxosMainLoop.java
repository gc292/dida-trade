package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.FastPhaseOneResponseProcessor;
import didatrade.util.PhaseTwoAttempt;
import didatrade.util.PhaseTwoResponseProcessor;

import java.util.*;

public final class FastPaxosMainLoop implements ConsensusLoop {
    private static final long RETRY_DELAY_MILLIS = 500;

    private final DidaTradeServerState serverState;

    private int     preparedBallot      = -1;
    private int     phaseOneRetryBallot = -1;
    private long    phaseOneRetryNotBefore;
    private boolean hasWork;
    private int     nextToOpen;

    // FAST-07 will advance this cursor while executing the decided prefix.
    private       int                   nextToExecute;
    private       int                   recoveryLimit;
    private final TreeSet<Integer>      recoveryHoles = new TreeSet<>();
    private final Map<Integer, Integer> holeProposals = new HashMap<>();

    private final Map<ProposalKey, Long> retryNotBefore  = new HashMap<>();
    private final Set<ProposalKey>       openedInstances = new HashSet<>();

    private Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries =
            new HashMap<>();

    private final Map<ProposalKey, ProposalAttempt> phaseTwoAttempts =
            new HashMap<>();

    private record ProposalKey(int ballot, int instance) {
    }

    private enum ProposalKind {
        CONCRETE, ANY
    }

    private record ProposalAttempt(ProposalKind kind, Integer value, PhaseTwoAttempt attempt) {
    }


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
        hasWork = true;
        notifyAll();
    }

    synchronized boolean takeWork() {
        boolean work = hasWork;
        hasWork = false;
        return work;
    }

    // FAST-08 uses this delay for its timed wait; -1 means no current retry.
    long nextRetryDelayMillis() {
        int ballot = serverState.getCurrentBallot();
        if (serverState.scheduler.leader(ballot) != serverState.my_id) return -1;

        long now   = System.currentTimeMillis();
        long delay = Long.MAX_VALUE;
        if (phaseOneRetryBallot == ballot) {
            delay = Math.max(0, phaseOneRetryNotBefore - now);
        }
        if (preparedBallot == ballot) {
            for (Map.Entry<ProposalKey, Long> retry : retryNotBefore.entrySet()) {
                if (retry.getKey().ballot() == ballot
                        && !serverState.paxos_log.isDecided(retry.getKey().instance())) {
                    delay = Math.min(delay, Math.max(0, retry.getValue() - now));
                }
            }
        }
        return delay == Long.MAX_VALUE ? -1 : delay;
    }

    public boolean prepareLeadership(int ballot) {
        if (serverState.getCurrentBallot() != ballot || serverState.scheduler.leader(ballot) != serverState.my_id)
            return false;

        if (preparedBallot == ballot) return true;

        if (phaseOneRetryBallot == ballot
                && System.currentTimeMillis() < phaseOneRetryNotBefore) return false;

        FastPhaseOneResponseProcessor processor = serverState.runFastPhaseOne(ballot);

        if (!processor.getAccepted()) {
            serverState.setCurrentBallot(processor.getMaxballot());
            if (serverState.getCurrentBallot() == ballot) {
                phaseOneRetryBallot    = ballot;
                phaseOneRetryNotBefore = System.currentTimeMillis() + RETRY_DELAY_MILLIS;
            }
            return false;
        }

        if (serverState.getCurrentBallot() != ballot) return false;

        recoveredEntries    = processor.getRecoveredEntries();
        phaseOneRetryBallot = -1;
        retryNotBefore.clear();
        holeProposals.clear();
        openedInstances.clear();
        recoveryHoles.clear();
        recoveryLimit = nextToExecute;

        Map<Integer, Integer> decided = serverState.paxos_log.getDecidedEntriesFrom(nextToExecute);
        for (int position : decided.keySet()) {
            recoveryLimit = Math.max(recoveryLimit, position + 1);
        }
        Iterator<Map.Entry<Integer, DidaTradePaxos.AcceptedEntry>> entries =
                recoveredEntries.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<Integer, DidaTradePaxos.AcceptedEntry> entry = entries.next();
            if (entry.getKey() < nextToExecute || decided.containsKey(entry.getKey())) {
                entries.remove();
            } else {
                recoveryLimit = Math.max(recoveryLimit, entry.getKey() + 1);
            }
        }
        for (int position = nextToExecute; position < recoveryLimit; position++) {
            if (!decided.containsKey(position) && !recoveredEntries.containsKey(position)) {
                recoveryHoles.add(position);
            }
        }
        nextToOpen     = recoveryLimit;
        preparedBallot = ballot;
        return true;
    }

    void openNextInstances(int ballot) {
        if (preparedBallot != ballot
                || serverState.getCurrentBallot() != ballot
                || serverState.scheduler.leader(ballot) != serverState.my_id) {
            return;
        }

        if (!recoveredEntries.isEmpty()
                || !holeProposals.isEmpty()
                || !recoveryHoles.isEmpty()) {
            return;
        }

        while (serverState.paxos_log.isDecided(nextToOpen)) {
            openedInstances.remove(new ProposalKey(ballot, nextToOpen));
            nextToOpen++;
        }
        ProposalKey key = new ProposalKey(ballot, nextToOpen);

        if (openedInstances.contains(key) || phaseTwoAttempts.containsKey(key)) {
            return;
        }

        Long retryTime = retryNotBefore.get(key);

        if (retryTime != null && System.currentTimeMillis() < retryTime) {
            return;
        }

        PhaseTwoAttempt attempt =
                serverState.startFastAnyIfCurrentAndUndecided(nextToOpen, ballot);

        if (attempt == null) {
            return;
        }

        phaseTwoAttempts.put(key, new ProposalAttempt(ProposalKind.ANY, null, attempt));

        retryNotBefore.remove(key);
        attempt.onCompletion(this::wakeup);

    }

    void proposePendingRequests(int ballot) {
        if (serverState.getCurrentBallot() != ballot
                || !serverState.scheduler.isacceptor(serverState.my_id, ballot)) {
            return;
        }

        Set<Integer> excludedIds = new HashSet<>();

        excludedIds.addAll(holeProposals.values());

        for (DidaTradePaxos.AcceptedEntry entry : recoveredEntries.values()) {
            excludedIds.add(entry.getValue());
        }

        for (DidaTradePaxos.AcceptedEntry entry
                : serverState.paxos_log.getAcceptedEntries()) {

            if (!serverState.paxos_log.isDecided(entry.getInstance())) {
                excludedIds.add(entry.getValue());
            }
        }

        excludedIds.addAll(
                serverState.paxos_log.getDecidedEntriesFrom(nextToExecute).values());

        while (serverState.getCurrentBallot() == ballot) {
            Integer instance = serverState.paxos_log.getNextFastInstance(ballot, nextToExecute);
            if (instance == null) return;

            RequestRecord request = serverState.req_history.getFirstPendingExcluding(excludedIds);
            if (request == null) return;

            int value = request.getId();
            DidaTradeServerState.PhaseTwoAcceptorResult result =
                    serverState.acceptFastProposal(instance, ballot, value);
            if (!result.accepted()) return;

            excludedIds.add(value);
            serverState.notifyLearners(instance, ballot, value);
        }
    }

    void startRecoveredProposals(int ballot) {
        if (preparedBallot != ballot
                || serverState.getCurrentBallot() != ballot
                || serverState.scheduler.leader(ballot) != serverState.my_id) {
            return;
        }

        // Reserve real pending requests for holes; never manufacture a command ID.
        Set<Integer> excludedIds = new HashSet<>(holeProposals.values());
        for (DidaTradePaxos.AcceptedEntry entry : recoveredEntries.values()) {
            excludedIds.add(entry.getValue());
        }
        excludedIds.addAll(serverState.paxos_log.getDecidedEntriesFrom(nextToExecute).values());
        for (DidaTradePaxos.AcceptedEntry entry : serverState.paxos_log.getAcceptedEntries()) {
            if (!serverState.paxos_log.isDecided(entry.getInstance())) {
                excludedIds.add(entry.getValue());
            }
        }
        Iterator<Integer> holes = recoveryHoles.iterator();
        while (holes.hasNext()) {
            int position = holes.next();
            if (serverState.paxos_log.isDecided(position)) {
                holes.remove();
                continue;
            }
            RequestRecord request = serverState.req_history.getFirstPendingExcluding(excludedIds);
            if (request == null) break;
            holeProposals.put(position, request.getId());
            excludedIds.add(request.getId());
            holes.remove();
        }

        TreeSet<Integer> positions = new TreeSet<>(recoveredEntries.keySet());
        positions.addAll(holeProposals.keySet());
        for (int instance : positions) {
            if (serverState.paxos_log.isDecided(instance)) {
                recoveredEntries.remove(instance);
                holeProposals.remove(instance);
                continue;
            }
            int value = recoveredEntries.containsKey(instance)
                    ? recoveredEntries.get(instance).getValue() : holeProposals.get(instance);

            ProposalKey key = new ProposalKey(ballot, instance);

            if (phaseTwoAttempts.containsKey(key)) {
                continue;
            }

            Long retryTime = retryNotBefore.get(key);

            if (retryTime != null && System.currentTimeMillis() < retryTime) {
                continue;
            }

            PhaseTwoAttempt attempt =
                    serverState.startFastPhaseTwoIfCurrentAndUndecided(
                            instance, ballot, value);

            if (attempt == null) {
                continue;
            }

            phaseTwoAttempts.put(key, new ProposalAttempt(ProposalKind.CONCRETE, value, attempt));
            retryNotBefore.remove(key);
            attempt.onCompletion(this::wakeup);
        }
    }

    void processCompletedAttempts() {

        Iterator<Map.Entry<ProposalKey, ProposalAttempt>> iterator =
                phaseTwoAttempts.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<ProposalKey, ProposalAttempt> entry = iterator.next();

            ProposalKey     key      = entry.getKey();
            ProposalAttempt proposal = entry.getValue();

            if (!proposal.attempt().isDone()) {
                continue;
            }

            PhaseTwoResponseProcessor result = proposal.attempt().getResult();

            iterator.remove();

            if (!result.getAccepted()) {
                serverState.setCurrentBallot(result.getMaxballot());

                if (serverState.getCurrentBallot() == key.ballot()
                        && preparedBallot == key.ballot()
                        && serverState.scheduler.leader(key.ballot()) == serverState.my_id
                        && !serverState.paxos_log.isDecided(key.instance())) {

                    retryNotBefore.put(
                            key,
                            System.currentTimeMillis() + RETRY_DELAY_MILLIS);
                }

                continue;
            }

            if (proposal.kind() == ProposalKind.ANY) {
                retryNotBefore.remove(key);

                if (serverState.getCurrentBallot() == key.ballot()
                        && preparedBallot == key.ballot()
                        && !serverState.paxos_log.isDecided(key.instance())) {
                    openedInstances.add(key);
                }

                continue;
            }

            serverState.paxos_log.decideEntry(
                    key.instance(), key.ballot(), proposal.value());

            recoveredEntries.remove(key.instance());
            holeProposals.remove(key.instance());
            recoveryHoles.remove(key.instance());
            Iterator<ProposalKey> retries = retryNotBefore.keySet().iterator();
            while (retries.hasNext()) {
                if (retries.next().instance() == key.instance()) retries.remove();
            }

            serverState.updateCompletedBallot(key.ballot());

        }
    }

    void executeDecidedPrefix() {
        while (true) {
            Integer commandId =
                    serverState.paxos_log.getDecidedCommandId(nextToExecute);

            if (commandId == null) {
                return;
            }

            if (serverState.req_history.getIfProcessed(commandId) != null) {
                nextToExecute++;
                continue;
            }

            RequestRecord request =
                    serverState.req_history.getIfPending(commandId);

            if (request == null) {
                return;
            }

            serverState.executeRequest(request);
            nextToExecute++;
        }
    }

    public void onCollision(int instance, int ballot) {
        // TODO FAST-09: publish a collision event; transition only once from current b to b+1.
        throw new UnsupportedOperationException("TODO FAST-09: collision handling");
    }
}
