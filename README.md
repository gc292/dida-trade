# DAD2627

DAD 26-27 Project

This repository holds the base code required to implement the project. Students are free to improve on the following
code.
Students should create a copy of this repository by clicking on `Use This Template` on the project GitHub page.

# ⚠️ **WARNING:** ⚠️

Students must **NOT** develop their solution on a Fork of the repository.
Forks are public and your code **WILL** be visible to other students.

It falls onto the students to **NOT DEVELOP ON A FORK THE PROJECT**

# Requirements

The project requires the following packages (you are free to use other versions of java):

- Java 22
- Maven 3.8.4
- Protoc 3.12

## Environment

The project includes a template `setup_env.sh` which students may use as basis to download all necessary packages for
the project execution.
The current script was designed for Linux/Ubuntu distributions on an Intel x86 environments.
Students may extend this script for their target distribution/CPU architecture.

If so, it will keep all required packages in `INSTALL_DIR`, which can be activated by running
`source INSTALL_DIR/env.sh`.

# Compiling

To compile the project, students must run the command
`mvn clean install` in the root directory

## ⚠️ ️**WARNING - Compilation Environment** ⚠️

The project requires different `pom.xml` for the *contract* module depending on the CPU architecture/OS distribution.
The project currently has two pre-pepared poms:

- One for **ARM/M4 Mac-OS** based systems, named `arm-pom.xml`;
- One for **Intel/Linux** based systems, named `intel-pom.xml`;

Before the first compilation, copy your required contract pom and rename it to `pom.xml`.

# Deployment

Current implementation assumes that all modules run on the same physical machine and requires 5 active servers.

The project is composed of three main components:

- Servers
- App (Clients)
- Console

## Servers

The servers run the base implementation. They are executed running the following command in the *server* directory:

`mvn exec:java -Dexec.args="{port} {id} {scheduler}"`

Where you must fill in the following arguments:

- **{port}**: Base port of all servers. **All servers should use the same port**. The Server binded port will be  *
  *{port} + {id}**.
- **{id}**: Sequential id of the server. Current implementation requires servers to be ID'ed starting from *0* to *N-1*
  servers.
- **{scheduler}**: The scheduler used for reconfiguration (use 'A' to start)

## Client

A client that executes transactions. It is executed by running the following command in the *app* directory:

`mvn exec:java -Dexec.args="{id} {host} {port} {scheduler}"`

Where you must fill in the following arguments:

- **{id}**: Sequential id of the client. Current implementation requires clients to be ID'ed starting from *1*.
- **{host}**: The host for all servers. (just use "localhost")
- **{port}**: Base port of all servers.
- **{scheduler}**: The scheduler used (use 'A' to start)

The client module opens a terminal from where students may issue commands. The following commands are available:

- `help` - Shows the full command list;
- `exit` - Gracefully finishes the client.

## Console

The console client servers as a front-end to issue configuration settings to servers. It is executed by running the
following command in the *consoleclient* directory:

`mvn exec:java -Dexec.args="{host} {port} {scheduler}"`

here you must fill in the following arguments:

- **{host}**: The host for all servers. (just use "localhost")
- **{port}**: Base port of all servers.
- **{scheduler}**: The scheduler used (use 'A' to start)

The console client opens a terminal from where students may issue configuration changes to servers. The following
commands are available:

- `help` - Shows the full command list;
- `ballot ballot_number server` - Instructs a server to start ballot with number 'ballot_number';
- `debug mode replica_id` - Activates debug on a given replica;
- `exit` - Gracefully finishes the console.

## Protobuffs and Utils

To support these modules, the project has additional directories:

- *contract*, holding the required `.proto` files;
- *util*, holding the general classes to collect RPC responses
- *core*, holding the general classes that manage meetings
- *configs*, holding the general classes that manage configurations

## How to Run

First, build the project:

```bash
source dev_env/env.sh
mvn clean install
```

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
  

## Shared code formatting

The root `.editorconfig` defines the team's formatting rules: UTF-8, LF line
endings, four spaces per indentation level, aligned Java field declarations and
aligned consecutive assignments. Keep this file in Git so everyone receives the
same rules when cloning or pulling the repository.

In IntelliJ IDEA, enable **Settings > Editor > Code Style > Enable EditorConfig
support**. Use **Code > Reformat Code** to format a file or selected code. These
rules also apply when the project is opened from a parent directory such as `DAD`.
For automatic formatting, enable **Tools > Actions on Save > Reformat code** in
your own IDE settings.

The `ij_java_*` alignment settings require IntelliJ IDEA or a compatible JetBrains
formatter. Other editors may only apply the standard EditorConfig rules. These
settings control formatting; they do not add lint checks to the Maven build.
Existing files are reformatted when you explicitly run the formatter.
