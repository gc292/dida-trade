package didatrade.server;

import didatrade.DidaTradePaxos;
import didatrade.util.FastPhaseOneResponseProcessor;
import didatrade.util.PhaseTwoAttempt;
import didatrade.util.PhaseTwoResponseProcessor;

import java.util.*;

public final class FastPaxosMainLoop implements ConsensusLoop {

    private static final long RETRY_DELAY_MILLIS = 500;
    private final DidaTradeServerState serverState;
    private final int windowSize;

    // Only the coordinator thread changes proposals, attempts and retry schedules.
    private int preparedBallot = -1;
    private int phaseOneRetryBallot = -1;
    private long phaseOneRetryNotBefore;
    private int nextToOpen;
    private int nextToExecute;
    private final TreeSet<Integer> recoveryHoles = new TreeSet<>();
    private final TreeMap<Integer, Integer> recoveryProposals = new TreeMap<>();
    // An older attempt may still yield a valid certificate after leadership changes.
    private final Map<ProposalKey, ProposalAttempt> phaseTwoAttempts = new HashMap<>();
    private final Map<ProposalKey, Long> retryNotBefore = new HashMap<>();
    private final Set<ProposalKey> openedInstances = new HashSet<>();
    private int notificationBallot = -1;
    private boolean notificationInFlight;
    private long notificationRetryNotBefore;

    // RPC callbacks only publish events under this monitor, then wake the coordinator.
    private boolean hasWork;
    private final Queue<Collision> collisions = new ArrayDeque<>();
    private final Set<ProposalKey> authorizationFailures = new HashSet<>();
    private final Queue<LeaderNotification> leaderNotifications = new ArrayDeque<>();

    private record Collision(int instance, int ballot) {}
    private record LeaderNotification(int ballot, boolean acknowledged) {}

    private record ProposalKey(int ballot, int instance) {
    }

    private enum ProposalKind {
        CONCRETE, ANY
    }

    private record ProposalAttempt(ProposalKind kind, Integer value, PhaseTwoAttempt attempt) {
    }


    public FastPaxosMainLoop(DidaTradeServerState serverState) {
        this(serverState, 1);
    }

    public FastPaxosMainLoop(DidaTradeServerState serverState, int windowSize) {
        if (windowSize < 1) throw new IllegalArgumentException("Fast window must be positive");
        this.serverState = serverState;
        this.windowSize = windowSize;
    }

    @Override
    public void run() {
        wakeup(); // Prepare and open the initial window before any client request.
        while (!Thread.currentThread().isInterrupted()) {
            long delay = nextRetryDelayMillis();
            synchronized (this) {
                if (!hasWork && delay != 0) {
                    try {
                        if (delay < 0) wait();
                        else wait(Math.max(1, delay));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                takeWork();
            }

            processEvents();
            notifyNewLeader();
            processCompletedAttempts();
            executeDecidedPrefix();
            int ballot = serverState.getCurrentBallot();
            boolean prepared = prepareLeadership(ballot);
            // Preparation may overlap Learn and completion of an older certificate.
            processCompletedAttempts();
            executeDecidedPrefix();
            if (serverState.getCurrentBallot() != ballot) continue;
            if (prepared) {
                startRecoveredProposals(ballot);
                openNextInstances(ballot);
            }
            proposePendingRequests(ballot);
        }
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

    // A pending RPC wakes us on completion; only dispatchable retries need a timer.
    long nextRetryDelayMillis() {
        int ballot = serverState.getCurrentBallot();
        long now   = System.currentTimeMillis();
        long delay = notificationBallot == ballot && !notificationInFlight
                ? Math.max(0, notificationRetryNotBefore - now) : Long.MAX_VALUE;
        if (serverState.scheduler.leader(ballot) != serverState.my_id) {
            return delay == Long.MAX_VALUE ? -1 : delay;
        }
        if (phaseOneRetryBallot == ballot) {
            delay = Math.max(0, phaseOneRetryNotBefore - now);
        }
        if (preparedBallot == ballot) {
            for (Map.Entry<ProposalKey, Long> retry : retryNotBefore.entrySet()) {
                ProposalKey key = retry.getKey();
                if (key.ballot() == ballot
                        && !phaseTwoAttempts.containsKey(key)
                        && !serverState.paxos_log.isDecided(key.instance())) {
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

        Map<Integer, DidaTradePaxos.AcceptedEntry> recoveredEntries = processor.getRecoveredEntries();
        phaseOneRetryBallot = -1;
        retryNotBefore.clear();
        recoveryProposals.clear();
        openedInstances.clear();
        recoveryHoles.clear();
        int recoveryLimit = nextToExecute;

        Map<Integer, Integer> decided = serverState.paxos_log.getDecidedEntriesFrom(nextToExecute);
        for (int position : decided.keySet()) {
            recoveryLimit = Math.max(recoveryLimit, position + 1);
        }
        for (DidaTradePaxos.AcceptedEntry entry : recoveredEntries.values()) {
            int position = entry.getInstance();
            if (position < nextToExecute || decided.containsKey(position)) continue;
            recoveryProposals.put(position, entry.getValue());
            recoveryLimit = Math.max(recoveryLimit, position + 1);
        }
        for (int position = nextToExecute; position < recoveryLimit; position++) {
            if (!decided.containsKey(position) && !recoveryProposals.containsKey(position)) {
                recoveryHoles.add(position);
            }
        }
        nextToOpen     = recoveryLimit;
        preparedBallot = ballot;
        return true;
    }

    void openNextInstances(int ballot) {
        if (!isPreparedLeader(ballot)) return;

        releaseObsoleteReservations();

        if (!recoveryProposals.isEmpty() || !recoveryHoles.isEmpty()) {
            return;
        }

        while (serverState.paxos_log.isDecided(nextToOpen)) {
            openedInstances.remove(new ProposalKey(ballot, nextToOpen));
            nextToOpen++;
        }
        for (int instance = nextToOpen; instance < nextToOpen + windowSize; instance++) {
            if (serverState.paxos_log.isDecided(instance)) continue;
            ProposalKey key = new ProposalKey(ballot, instance);
            if (openedInstances.contains(key) && !retryNotBefore.containsKey(key)) continue;
            startProposal(instance, ballot, ProposalKind.ANY, null);
        }
    }

    void proposePendingRequests(int ballot) {
        if (serverState.getCurrentBallot() != ballot
                || !serverState.scheduler.isacceptor(serverState.my_id, ballot)) {
            return;
        }

        releaseObsoleteReservations();

        Set<Integer> excludedIds = new HashSet<>(recoveryProposals.values());
        excludedIds.addAll(serverState.paxos_log.getReservedCommandIds(ballot));

        excludedIds.addAll(
                serverState.paxos_log.getDecidedEntriesFrom(nextToExecute).values());

        while (serverState.getCurrentBallot() == ballot) {
            Integer instance = serverState.paxos_log.getNextFastInstance(ballot, nextToExecute);
            if (instance == null) return;

            RequestRecord request = serverState.req_history.getFirstPendingExcluding(excludedIds);
            if (request == null) return;

            int value = request.getId();
            try {
                serverState.applySlowMode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            DidaTradeServerState.PhaseTwoAcceptorResult result =
                    serverState.acceptFastProposal(instance, ballot, value);
            if (!result.accepted()) return;

            excludedIds.add(value);
            serverState.notifyLearners(instance, ballot, value);
        }
    }

    void startRecoveredProposals(int ballot) {
        if (!isPreparedLeader(ballot)) return;

        releaseObsoleteReservations();

        // Reserve real pending requests for holes; never manufacture a command ID.
        Set<Integer> excludedIds = new HashSet<>(recoveryProposals.values());
        excludedIds.addAll(serverState.paxos_log.getDecidedEntriesFrom(nextToExecute).values());
        excludedIds.addAll(serverState.paxos_log.getReservedCommandIds(ballot, recoveryHoles));
        Iterator<Integer> holes = recoveryHoles.iterator();
        while (holes.hasNext()) {
            int position = holes.next();
            if (serverState.paxos_log.isDecided(position)) {
                holes.remove();
                continue;
            }
            RequestRecord request = serverState.req_history.getFirstPendingExcluding(excludedIds);
            if (request == null) break;
            recoveryProposals.put(position, request.getId());
            excludedIds.add(request.getId());
            holes.remove();
        }

        for (Map.Entry<Integer, Integer> proposal : recoveryProposals.entrySet()) {
            startProposal(proposal.getKey(), ballot, ProposalKind.CONCRETE, proposal.getValue());
        }
    }

    private boolean isPreparedLeader(int ballot) {
        return preparedBallot == ballot
                && serverState.getCurrentBallot() == ballot
                && serverState.scheduler.leader(ballot) == serverState.my_id;
    }

    private void startProposal(int instance, int ballot, ProposalKind kind, Integer value) {
        ProposalKey key = new ProposalKey(ballot, instance);
        if (phaseTwoAttempts.containsKey(key)) return;
        Long retryTime = retryNotBefore.get(key);
        if (retryTime != null && System.currentTimeMillis() < retryTime) return;

        PhaseTwoAttempt attempt;
        if (kind == ProposalKind.ANY) {
            attempt = serverState.startFastAnyIfCurrentAndUndecided(instance, ballot);
        } else {
            attempt = serverState.startFastPhaseTwoIfCurrentAndUndecided(instance, ballot, value);
        }
        if (attempt == null) return;

        // Register first: an already-completed attempt invokes its callback immediately.
        phaseTwoAttempts.put(key, new ProposalAttempt(kind, value, attempt));
        retryNotBefore.remove(key);
        attempt.onCompletion(this::wakeup);
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

                if (isPreparedLeader(key.ballot())
                        && !serverState.paxos_log.isDecided(key.instance())) {

                    retryNotBefore.put(
                            key,
                            System.currentTimeMillis() + RETRY_DELAY_MILLIS);
                }

                continue;
            }

            if (proposal.kind() == ProposalKind.ANY) {
                // Keep a retry published by a failed fifth recipient after quorum completion.
                if (isPreparedLeader(key.ballot())
                        && !serverState.paxos_log.isDecided(key.instance())) {
                    openedInstances.add(key);
                }

                continue;
            }

            serverState.paxos_log.decideEntry(
                    key.instance(), key.ballot(), proposal.value());

            serverState.updateCompletedBallot(key.ballot());
        }
        releaseObsoleteReservations();
    }

    void executeDecidedPrefix() {
        releaseObsoleteReservations();

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

    private void releaseObsoleteReservations() {
        Iterator<Map.Entry<Integer, Integer>> proposals = recoveryProposals.entrySet().iterator();
        while (proposals.hasNext()) {
            Map.Entry<Integer, Integer> proposal = proposals.next();
            int position = proposal.getKey();
            if (serverState.paxos_log.isDecided(position)
                    || serverState.paxos_log.isReservationSuperseded(position, preparedBallot, proposal.getValue())) {
                proposals.remove();
                ProposalKey key = new ProposalKey(preparedBallot, position);
                retryNotBefore.remove(key);
                openedInstances.remove(key);
            }
        }
        Iterator<Integer> holes = recoveryHoles.iterator();
        while (holes.hasNext()) {
            if (serverState.paxos_log.isDecided(holes.next())) holes.remove();
        }
        Iterator<ProposalKey> retries = retryNotBefore.keySet().iterator();
        while (retries.hasNext()) {
            if (serverState.paxos_log.isDecided(retries.next().instance())) retries.remove();
        }
        Iterator<ProposalKey> opened = openedInstances.iterator();
        while (opened.hasNext()) {
            if (serverState.paxos_log.isDecided(opened.next().instance())) opened.remove();
        }
    }

    synchronized void onAuthorizationFailure(int instance, int ballot) {
        authorizationFailures.add(new ProposalKey(ballot, instance));
        wakeup();
    }

    public synchronized void onCollision(int instance, int ballot) {
        collisions.add(new Collision(instance, ballot));
        wakeup();
    }

    void processEvents() {
        List<Collision> events;
        List<LeaderNotification> replies;
        Set<ProposalKey> failures;
        synchronized (this) {
            events = new ArrayList<>(collisions);
            collisions.clear();
            replies = new ArrayList<>(leaderNotifications);
            leaderNotifications.clear();
            failures = new HashSet<>(authorizationFailures);
            authorizationFailures.clear();
        }
        for (Collision event : events) {
            if (serverState.paxos_log.isDecided(event.instance())) continue;
            if (serverState.advanceFastBallotIfCurrent(event.ballot())) {
                notificationBallot = event.ballot() + 1;
                notificationInFlight = false;
                notificationRetryNotBefore = 0;
            }
        }
        for (ProposalKey key : failures) {
            if (isPreparedLeader(key.ballot()) && !serverState.paxos_log.isDecided(key.instance())) {
                retryNotBefore.put(key, System.currentTimeMillis() + RETRY_DELAY_MILLIS);
            }
        }
        for (LeaderNotification reply : replies) {
            if (reply.ballot() != notificationBallot) continue;
            notificationInFlight = false;
            if (reply.acknowledged()) notificationBallot = -1;
            else notificationRetryNotBefore = System.currentTimeMillis() + RETRY_DELAY_MILLIS;
        }
    }

    private void notifyNewLeader() {
        if (notificationBallot < 0) return;
        if (notificationBallot != serverState.getCurrentBallot()
                || serverState.scheduler.leader(notificationBallot) == serverState.my_id) {
            notificationBallot = -1;
            return;
        }
        if (notificationInFlight || System.currentTimeMillis() < notificationRetryNotBefore) return;
        int ballot = notificationBallot;
        notificationInFlight = true;
        serverState.notifyFastLeader(ballot, acknowledged -> {
            synchronized (this) {
                leaderNotifications.add(new LeaderNotification(ballot, acknowledged));
                wakeup();
            }
        });
    }
}
