# DIDA Trade

Project developed for the **Distributed Applications Development (DAD)** course, 2026/2027.

DIDA Trade is a distributed trading system implemented in **Java** and **gRPC**. The project explores state machine
replication and consensus protocols based on **Paxos**, progressively extending the baseline implementation with
Multi-Paxos, Fast Paxos, causal operations, and dynamic membership.

## Requirements

The development environment uses:

* Java 22
* Maven 3.8.4
* Protocol Buffers Compiler (`protoc`) 3.12.0
* gRPC Java

The repository includes a development environment under:

```text
dev_env/
```

Load it from the repository root before compiling or running the project:

```bash
source dev_env/env.sh
```

> The provided environment is configured for Linux x86-64. Paths and platform-specific dependencies may need to be
> adjusted when running on another operating system or CPU architecture.

## Project Structure

The project is organized as a multi-module Maven project:

```text
DIDA-Trade/
├── app/        # Transaction client
├── configs/    # Replica configurations and schedules
├── console/    # Administrative console
├── contract/   # Protocol Buffer and gRPC contracts
├── core/       # Trading domain and state machine logic
├── server/     # Replicas and Paxos implementation
├── util/       # Shared utilities and RPC response processing
├── scripts/    # Linux/WSL setup and launch scripts
├── dev_env/    # Development environment
└── Docs/       # Project documentation and specification
```

### Main Components

**Server**

Runs a replica of the distributed trading system. Replicas participate in the consensus protocol and execute decided
operations against their local state.

**App**

Interactive client used to submit trading operations to the replicated system.

**Console**

Administrative interface used to control replicas, initiate ballots, activate replicas, and enable debugging behavior.

### Supporting Modules

* `contract` — `.proto` definitions and generated Java/gRPC interfaces.
* `configs` — replica membership, leaders, quorums, ballots, and configuration schedules.
* `core` — users, balances, stocks, orders, and trade matching.
* `util` — common utilities and asynchronous RPC response processing.

## Building

From the repository root:

```bash
source dev_env/env.sh
mvn clean install
```

This builds the complete multi-module project and generates the required Protocol Buffers and gRPC classes.

### Platform-specific protobuf configuration

The `contract` module depends on platform-specific Protocol Buffers and gRPC executables.

The repository provides configurations for the supported development environments. Make sure that `contract/pom.xml`
references binaries compatible with your operating system and CPU architecture before compiling.

The current development configuration targets **Linux x86-64**.

## Helper Scripts (Linux / WSL)

From the repository root, install the development tools if needed:

```bash
bash scripts/setup_env.sh
```

The tools are installed into the repository's `dev_env/` directory. Both scripts
resolve paths relative to their own location, regardless of the current directory.
On Windows, run them inside WSL; they are also usable directly on Linux.

Build and start each component in a separate terminal:

```bash
bash scripts/run.sh build
bash scripts/run.sh s0
bash scripts/run.sh s1
bash scripts/run.sh s2
bash scripts/run.sh cliente
bash scripts/run.sh consola
```

The launcher loads `dev_env/env.sh` automatically and uses Schedule A, base port
9000, and client ID 1. Each command runs in the current terminal.

## Running the System

The current deployment assumes that all components run on the same physical machine.

With **Schedule A**, the system consists of three replicas.

Start the servers before starting clients.

### Servers

From the `server/` directory:

```bash
mvn exec:java -Dexec.args="{port} {id} {scheduler} [classic|multi|fast]"
```

Arguments:

* `{port}` — base port shared by all replicas.
* `{id}` — replica identifier, starting at `0`.
* `{scheduler}` — configuration schedule (`A` for the initial configuration).
* Protocol mode is optional and defaults to `classic`; use `fast` with Schedule B.

Step 2 implements Multi-Paxos with Schedule A. The `multi` mode performs Phase 1
once per ballot, recovers previously accepted values by log position, and allows
multiple Phase 2 instances to overlap without waiting for earlier instances to decide.
Decisions may be learned out of order, while commands are executed strictly in log
order. Leader changes recover accepted values through Phase 1, and failed Phase 1 or
Phase 2 attempts are retried without busy-waiting.

The `classic` mode remains available and runs the original per-instance Paxos protocol.
Existing `scripts/run.sh s0`, `s1`, and `s2` commands select Classic with Schedule A.
To run Multi-Paxos directly, pass `multi` as the optional protocol argument.

Step 3 provides Fast Paxos using `B fast`: start five replicas with IDs `0` to `4`.
Fast decisions and recovery require four acceptors. The leader prepares each ballot
and opens a window of four positions with ANY; acceptors vote directly on pending client
requests. Proven collisions automatically start the next ballot and notify its leader.
Timeouts retry the current ballot without triggering an election. Losing requests retain
their arrival priority, and execution waits for the decided prefix and command payload.
Fast mode requires Schedule B. In this mode, the console's new-ballot reply acknowledges
the change immediately; its completed-ballot field still reports actual decisions.

```bash
# Run each replica in a separate terminal, from server/ with the environment loaded.
mvn exec:java -Dexec.args="9000 0 B fast"
# Repeat with IDs 1, 2, 3 and 4; configure clients and console with Schedule B.
```

The actual port used by a replica is:

```text
port + id
```

For example, using base port `9000`:

```text
Replica 0 -> 9000
Replica 1 -> 9001
Replica 2 -> 9002
```

### Client

From the `app/` directory:

```bash
mvn exec:java -Dexec.args="{id} {host} {port} {scheduler}"
```

Arguments:

* `{id}` — client identifier, starting at `1`.
* `{host}` — host where the replicas are running (`localhost` for local execution).
* `{port}` — base replica port.
* `{scheduler}` — configuration schedule.

The client provides an interactive terminal for issuing operations against the trading system.

Use:

```text
help
```

to display the available commands.

Use:

```text
exit
```

to terminate the client gracefully.

### Console

From the `console/` directory:

```bash
mvn exec:java -Dexec.args="{host} {port} {scheduler}"
```

Arguments:

* `{host}` — host where the replicas are running.
* `{port}` — base replica port.
* `{scheduler}` — configuration schedule.

The console provides administrative commands for controlling the distributed system.

Examples include:

```text
help
ballot <ballot_number> <server>
debug <mode> <replica_id>
exit
```

Use `help` for the complete list of commands supported by the current implementation.

## Quick Start

The following example starts a complete local deployment using **Schedule A** and base port `9000`.

First, build the project:

```bash
source dev_env/env.sh
mvn clean install
```

Then open separate terminals for each component.

### Server 0

```bash
source dev_env/env.sh
cd server
mvn exec:java -Dexec.args="9000 0 A"
```

### Server 1

```bash
source dev_env/env.sh
cd server
mvn exec:java -Dexec.args="9000 1 A"
```

### Server 2

```bash
source dev_env/env.sh
cd server
mvn exec:java -Dexec.args="9000 2 A"
```

### Client

```bash
source dev_env/env.sh
cd app
mvn exec:java -Dexec.args="1 localhost 9000 A"
```

### Console

```bash
source dev_env/env.sh
cd console
mvn exec:java -Dexec.args="localhost 9000 A"
```

## Debugging

The console can configure debugging behavior on individual replicas using:

```text
debug <mode> <replica_id>
```

The currently implemented modes are:

| Mode | Behavior                 |
|-----:|--------------------------|
|  `1` | Crash replica            |
|  `2` | Freeze replica           |
|  `3` | Unfreeze replica         |
|  `4` | Enable artificial delay  |
|  `5` | Disable artificial delay |

Artificial delay introduces a random delay between **100 ms and 1000 ms** when processing requests.

These modes are useful for testing the behavior of the distributed protocol under replica failures, delayed
communication, and partial availability.

## Code Formatting

The root `.editorconfig` defines the formatting rules shared by the team.

Among other settings, it defines:

* UTF-8 encoding
* LF line endings
* Four spaces per indentation level
* Java field alignment
* Consecutive assignment alignment

The `.editorconfig` file should remain version-controlled so that all contributors use the same formatting rules.

### IntelliJ IDEA

Enable EditorConfig support under:

```text
Settings > Editor > Code Style > Enable EditorConfig support
```

To format code manually:

```text
Code > Reformat Code
```

Automatic formatting on save can optionally be enabled under:

```text
Tools > Actions on Save > Reformat code
```

Some `ij_java_*` options are specific to IntelliJ IDEA or compatible JetBrains formatters. Other editors may only apply
the standard EditorConfig properties.

Formatting rules affect source-code formatting only; they do not introduce additional linting or Maven validation.
