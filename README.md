# DIDA Trade

Project developed for the **Distributed Applications Development (DAD)** course, 2026/2027.

DIDA Trade is a distributed trading system implemented in **Java** and **gRPC**. The project explores state machine replication and consensus protocols based on **Paxos**, progressively extending the baseline implementation with Multi-Paxos, Fast Paxos, causal operations, and dynamic membership.

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

> The provided environment is configured for Linux x86-64. Paths and platform-specific dependencies may need to be adjusted when running on another operating system or CPU architecture.

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

Runs a replica of the distributed trading system. Replicas participate in the consensus protocol and execute decided operations against their local state.

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

The repository provides configurations for the supported development environments. Make sure that `contract/pom.xml` references binaries compatible with your operating system and CPU architecture before compiling.

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
mvn exec:java -Dexec.args="{port} {id} {scheduler}"
```

Arguments:

* `{port}` — base port shared by all replicas.
* `{id}` — replica identifier, starting at `0`.
* `{scheduler}` — configuration schedule (`A` for the initial configuration).

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
| ---: | ------------------------ |
|  `1` | Crash replica            |
|  `2` | Freeze replica           |
|  `3` | Unfreeze replica         |
|  `4` | Enable artificial delay  |
|  `5` | Disable artificial delay |

Artificial delay introduces a random delay between **100 ms and 1000 ms** when processing requests.

These modes are useful for testing the behavior of the distributed protocol under replica failures, delayed communication, and partial availability.

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

Some `ij_java_*` options are specific to IntelliJ IDEA or compatible JetBrains formatters. Other editors may only apply the standard EditorConfig properties.

Formatting rules affect source-code formatting only; they do not introduce additional linting or Maven validation.
