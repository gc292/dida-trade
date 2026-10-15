# DIDA-Trade

Distributed trading service developed for the **Distributed Applications Design (DAD)** course at **Instituto Superior Técnico**, 2026/2027. Implemented in Java with gRPC and Protocol Buffers.

## Implemented scope

| Step | Functionality | Deployment |
|---|---|---|
| 1 | Classic Paxos: Phase 1 and Phase 2 per log position | Schedule A, 3 replicas |
| 2 | Multi-Paxos: preparation per ballot and overlapping Phase 2 instances | Schedule A, 3 replicas |
| 3 | Fast Paxos: direct acceptor voting, ANY authorization and collision recovery | Schedule B, 5 replicas |
| 4 | Causally consistent local `BALANCE`, bypassing consensus | All supported modes |
| 5 | Dynamic membership and log-based state transfer using Multi-Paxos | Schedule C, 7 replicas; partial assignment implementation |

Schedule B uses five acceptors following the instructor's clarification. Fast decisions and recovery require four acceptors.

**Step 5 limitations:** Schedule C runs conventional Multi-Paxos throughout, including ballots marked Fast in the schedule. Mixed Fast/conventional reconfiguration and historical quorum retirement are not implemented.

## Requirements and build

The supplied development setup targets **Linux x86-64**, including Linux under WSL:

- Java 22
- Maven 3.8.4
- Protocol Buffers compiler 3.12.0

From the repository root, install the local tools if needed:

```bash
bash scripts/setup_env.sh
source dev_env/env.sh
mvn clean install
```

The setup script installs tools into `dev_env/` and generates `env.sh` with paths for the current checkout. Run it again after moving the repository if those paths are stale. Alternatively, use a compatible existing Java/Maven environment. The protobuf/gRPC executable configuration in `contract/pom.xml` targets Linux x86-64.

`mvn clean install` builds all modules, generates gRPC classes and runs the automated tests. Build the complete reactor before running components or server-only tests, since some integration tests also launch the production client.

## Running locally

Start each component in a **separate terminal**. Load the environment from the repository root before changing into a module:

```bash
source dev_env/env.sh
```

Use the same schedule and base port for every component in a deployment, and the same protocol mode for every replica. The server listens on **base port + replica ID**; for example, replica 2 uses port 9002 when the base port is 9000.

### 1. Start the replicas

From `server/`:

```bash
mvn exec:java -Dexec.args="<base-port> <replica-id> <schedule> <protocol>"
```

| Deployment | Schedule | Protocol | Replica IDs | Example: replica 0 |
|---|---|---|---|---|
| Classic Paxos | A | `classic` | 0–2 | `9000 0 A classic` |
| Multi-Paxos | A | `multi` | 0–2 | `9000 0 A multi` |
| Fast Paxos | B | `fast` | 0–4 | `9000 0 B fast` |
| Dynamic membership | C | `multi` | 0–6 | `9000 0 C multi` |

For example, start Schedule A/Multi in three terminals:

```bash
# Terminal 1, from server/
mvn exec:java -Dexec.args="9000 0 A multi"

# Terminal 2, from server/
mvn exec:java -Dexec.args="9000 1 A multi"

# Terminal 3, from server/
mvn exec:java -Dexec.args="9000 2 A multi"
```

The protocol argument defaults to `classic` when omitted. Fast mode requires Schedule B. For Schedule C, explicitly select `multi` and start all seven processes; the acceptor membership changes with the ballot, while all seven nodes are learners.

### 2. Start the client

From `app/`:

```bash
mvn exec:java -Dexec.args="1 localhost 9000 A"
```

Arguments are `<client-id> <host> <base-port> <schedule>`. Replace `A` with `B` or `C` to match the servers. Concurrent clients must use distinct IDs in **1–99**. Do not reuse a client ID with a reset sequence while replicas retain its earlier requests.

| Command | Purpose |
|---|---|
| `populate <n>` | Populate users through the replicated state machine |
| `add <uid> <wallet> <stock>` | Create a user |
| `buy <uid> <percentage>` | Submit a buy order using a percentage of the user's wallet |
| `sell <uid> <percentage>` | Submit a sell order using a percentage of the user's stock |
| `balance <uid>` | Read a local balance after satisfying the client's causal dependency |
| `show` | Submit a replicated state-dump command |
| `loopbuy`, `loopsell`, `looprandom` | Generate repeated operations |
| `lenght <n>` | Set the number of loop operations; spelling matches the implementation |
| `help`, `exit` | Display help or close the client |

The loop command names above match the dispatcher; the client's built-in help currently lists different names for those three commands.

For a small interactive example:

```text
add 20 100 100
add 21 100 100
sell 20 50
buy 21 50
balance 20
balance 21
```

### 3. Start the administrative console

From `console/`:

```bash
mvn exec:java -Dexec.args="localhost 9000 A"
```

Arguments are `<host> <base-port> <schedule>`; use the same schedule as the deployment.

| Command | Purpose |
|---|---|
| `ballot <number> <replica-id>` | Request a ballot at the selected replica |
| `activate <replica-id>` | Send the existing activation RPC |
| `debug <mode> <replica-id>` | Change a replica's debug behavior |
| `debug help` | Explain debug modes |
| `help`, `exit` | Display help or close the console |

Ballot numbers must follow the schedule's leader mapping:

| Ballot | Schedule A leader | Schedule B leader | Schedule C leader |
|---|---|---|---|
| 0 | 0 | 0 | 0 |
| 1 | 1 | 1 | 1 |
| 2 | 2 | 2 | 3 |
| 3 | 0 | 0 | 4 |
| 4 | 1 | 1 | 5 |

For example, after starting all seven Schedule C replicas, use the C console to request the new configuration's ballot:

```text
ballot 2 3
```

C changes acceptors from `{0,1,2,3,4}` in ballots 0–1 to `{2,3,4,5,6}` from ballot 2. Preparation still requires a quorum from each relevant historical configuration; a majority of the seven-node union alone is insufficient.

### Debug modes

| Mode | Behavior |
|---|---|
| 1 | Crash the replica process |
| 2 | Freeze the replica |
| 3 | Unfreeze the replica |
| 4 | Enable artificial request delay, randomly 100–1000 ms |
| 5 | Disable artificial delay |

Crashing a replica terminates it; restarting does not restore durable state. Freeze/unfreeze is the appropriate mechanism for temporary pauses in recovery demonstrations.

### Launcher shortcuts

The helper loads the environment automatically and runs one process in the current terminal:

```bash
bash scripts/run.sh build
```

For Schedule A/Classic, run each of these separately:

```bash
bash scripts/run.sh s0
bash scripts/run.sh s1
bash scripts/run.sh s2
bash scripts/run.sh cliente
bash scripts/run.sh consola
```

For Schedule C/Multi, use `c0` through `c6`, `clienteC` and `consolaC`. All shortcuts use base port 9000 and clients use ID 1. Use the explicit Maven commands for Schedule A/Multi or Schedule B/Fast.

## Architecture

| Module | Responsibility |
|---|---|
| `app` | Client broadcasts, request IDs and observed causal watermark |
| `configs` | Schedule membership, leaders and quorum policies |
| `console` | Administrative and debug commands |
| `contract` | Protocol Buffers definitions and generated gRPC interfaces |
| `core` | Users, wallets, stock and FIFO order matching |
| `server` | RPC services, three separate consensus coordinators, replicated log and execution |
| `util` | Shared RPC collectors, response processors and asynchronous attempt handling |

Classic, Multi and Fast keep independent coordinator loops. They share canonical request tracking, accepted/decided log state, learner processing and domain execution.

Clients broadcast command payloads, while consensus orders request IDs. A replica executes only the contiguous decided prefix and waits for missing payloads. Previously executed IDs do not execute again or advance the causal version. Learn delivery retries preserve the original vote and use bounded exponential backoff, with one active delivery per destination.

`BALANCE` uses the client's maximum observed version as a dependency. The replica waits until it has applied that history, then reads balance and version under the same domain-state lock. This supports causal reads across replicas without requiring the globally freshest state.

Schedule C transfers decided log entries with their payloads through the normal executor. Terminal transfer failures are retried, and later decisions or payload availability can extend the transferable prefix. Transfers currently resend complete prefixes rather than deltas.

## Validation

The last fully recorded root build passed **117 tests**, with no failures, errors or skipped tests. Coverage includes:

- Log and canonical request-history invariants.
- Classic and Multi consensus, recovery and ordered execution.
- Fast authorization, voting, collisions and finite-case recovery checks.
- Learn transport-failure recovery and Multi stale-retry regressions.
- Causal dependencies, cancellation and balance/version snapshots.
- State-transfer retries, late prefix growth, duplicate replay, freeze/unfreeze and seven-replica reconfiguration.

The suite combines unit, integration, real-gRPC and fault-injection tests. Passing it is evidence for the exercised scenarios, not a formal proof or a performance benchmark.

Run the complete validation from the repository root:

```bash
source dev_env/env.sh
mvn clean install
```

## Known limitations

- Schedule C does not execute Fast ballots and does not retire historical quorum dependencies.
- State is in memory: durable restart recovery, checkpoints and log compaction are not implemented.
- Correctness assumes non-Byzantine peers, stable request identities, eventual command-payload availability and usable quorums. Classic/Multi leader changes are requested administratively; Fast additionally advances ballots on proven collisions.
- Delivery retries address terminal RPC failures. A call that never completes still depends on eventual transport progress or unfreezing the receiver.
- Two inherited domain limitations remain: `populate` does not reinitialize existing users, and percentage calculations can overflow for large integer balances or stock values. Startup creates users 0–9 with wallet and stock 10.
- No comparative latency or throughput measurements are claimed.

## Authors

- Guilherme Caria — 121557
- Filipe Mesquita — 103531
- Sarthak Joshi — 119818
