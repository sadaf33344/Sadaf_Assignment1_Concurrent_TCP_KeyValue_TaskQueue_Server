# Concurrent TCP Key-Value & Task Queue Server
-
### COSC 3657 — Distributed Systems | Unit 1: Bare-Metal IPC & Custom Protocol Design

**Student:** Sadaf Kabir  
**Implementation Language:** Python 3  
**Development Environment:** GitHub Codespaces (Option B – Cloud Execution)  
**Communication:** TCP Sockets, Port 9090

---

## 1. Project Overview

For Unit 1 of Distributed Systems, I developed a concurrent TCP server that supports a key-value store and a task queue.

The main purpose of this assignment was to understand how processes communicate over a network without using high-level frameworks such as HTTP or gRPC.

I used Python's built-in socket library to create a TCP server and designed a custom communication protocol using a 4-byte length header followed by a JSON message.

The server can handle multiple clients using a thread pool. I also used locks and condition variables to protect shared data when different clients access the server at the same time.


### Project Objectives

- Create a TCP server using low-level socket programming.
- Design a custom length-prefixed communication protocol.
- Handle partial TCP reads correctly.
- Implement PUT and GET operations for a key-value store.
- Implement ENQUEUE and DEQUEUE operations for a task queue.
- Protect shared data using synchronization primitives.
- Support multiple concurrent clients using a thread pool.
- Test the system using 20 concurrent workers and additional edge cases.
---

## 2. System Architecture

The system follows a client-server architecture.

Clients send requests through TCP sockets. The server accepts incoming connections and assigns them to worker threads. The worker threads process requests and access shared storage.

```text
          CLIENT APPLICATIONS
       +----------+----------+
       |          |          |
    Client 1   Client 2   Client N
       |          |          |
       +----------+----------+
                  |
             TCP Port 9090
                  |
                  v
       +----------------------+
       |      TCP SERVER      |
       |      server.py       |
       +----------------------+
                  |
                  v
       +----------------------+
       |   ThreadPoolExecutor |
       |    Maximum: 10       |
       |    Worker Threads    |
       +----------------------+
                  |
                  v
       +----------------------+
       |    Request Handler   |
       | PUT / GET / ENQUEUE  |
       |       DEQUEUE        |
       +----------------------+
                  |
                  v
       +----------------------+
       |      storage.py      |
       |                      |
       |  Key-Value Store     |
       |  Python Dictionary   |
       |                      |
       |  Task Queue          |
       |  collections.deque   |
       |                      |
       |  Lock + Condition    |
       +----------------------+
```

### How the System Works

1. The server starts and listens for connections on port 9090.
2. A client connects to the server using a TCP socket.
3. The server assigns the connection to an available worker thread.
4. The client sends a request using the custom protocol.
5. The server reads the message, identifies the requested operation, and processes it.
6. Shared data is protected using locks and condition variables.
7. The server sends a response using the same custom protocol.
8. The client receives and processes the response.

---

## 3. Custom TCP Communication Protocol

### 3.1 What Is a Protocol?

A protocol is a set of rules and formats that processes follow to communicate and perform tasks.

For this assignment, I created a custom application-level protocol that defines how the client and server exchange messages.

My protocol specifies:

- **Transport:** TCP sockets.
- **Port:** 9090.
- **Message format:** 4-byte length header followed by a JSON payload.
- **Byte order:** Big-endian (network byte order).
- **Text encoding:** UTF-8.
- **Supported operations:** PUT, GET, ENQUEUE, and DEQUEUE.
- **Response format:** JSON objects containing a status and operation-specific data.
- **Error handling:** Invalid requests return an error response when possible.

Both the client and server must follow these rules to communicate successfully.

### 3.2 Why I Used a Custom Protocol

TCP provides a reliable stream of bytes, but it does not preserve individual message boundaries.

For example, if a client sends a complete message, the server might receive it in several smaller parts.

The server therefore cannot assume that one `recv()` call will return an entire message.

To solve this problem, I used length-prefixed framing.

### 3.3 Message Structure

Every message contains two parts:

```text
+------------------------+-----------------------------+
| 4-Byte Length Header   |       JSON Payload          |
+------------------------+-----------------------------+
| Unsigned Integer       | UTF-8 Encoded JSON          |
| Big-Endian             | Variable Length             |
+------------------------+-----------------------------+
```

**Header:** The first four bytes specify the number of bytes in the JSON payload.

**Payload:** The remaining bytes contain the JSON request or response.

The header uses Python's `struct` module with the format `!I`, representing an unsigned 32-bit integer in network byte order.

For example, the request:

```json
{"action": "GET", "key": "student"}
```

is converted into UTF-8 bytes. Its byte length is placed in the four-byte header before the payload is transmitted.

### 3.4 Sending a Message

The `send_message()` function in `protocol.py` performs three steps:

1. Converts the Python dictionary into a JSON string.
2. Encodes the JSON string into UTF-8 bytes.
3. Adds the four-byte length header and sends the complete frame using `sendall()`.

Using `sendall()` helps ensure that the complete message is transmitted unless a socket error occurs.

### 3.5 Receiving a Message

The `receive_message()` function:

1. Reads exactly four bytes for the header.
2. Converts the header into the payload length.
3. Checks whether the payload length is valid.
4. Reads exactly the required number of payload bytes.
5. Decodes the UTF-8 payload and parses the JSON.

### 3.6 Handling Partial TCP Reads

One important part of the protocol is the `recv_exact()` function.

A single `recv()` call might return fewer bytes than requested. Therefore, `recv_exact()` repeatedly reads data until the expected number of bytes has arrived.

If the connection closes before all required bytes arrive, the function raises a connection error.

This prevents incomplete messages from being treated as valid requests.

### 3.7 Message Size Validation

The server rejects message lengths of zero or greater than 1 MB.

This helps prevent the server from attempting to process excessively large payloads.

---

## 4. Supported Operations

The server supports four main operations.

| Operation | Purpose | Required Fields |
|---|---|---|
| PUT | Store a value using a key | `action`, `key`, `value` |
| GET | Retrieve a value using a key | `action`, `key` |
| ENQUEUE | Add a task to the queue | `action`, `task` |
| DEQUEUE | Remove and return a task | `action`; optional `timeout` |

### 4.1 PUT — Store Data

**Client request:**

```json
{
  "action": "PUT",
  "key": "student",
  "value": "Sadaf"
}
```

**Server response:**

```json
{
  "status": "ok"
}
```

The server stores the value in a shared Python dictionary.

### 4.2 GET — Retrieve Data

**Client request:**

```json
{
  "action": "GET",
  "key": "student"
}
```

**Server response:**

```json
{
  "status": "ok",
  "value": "Sadaf"
}
```

If a key does not exist, the server returns `null` as its value.

### 4.3 ENQUEUE — Add a Task

**Client request:**

```json
{
  "action": "ENQUEUE",
  "task": "Complete assignment"
}
```

**Server response:**

```json
{
  "status": "ok"
}
```

The task is added to the end of the shared queue.

### 4.4 DEQUEUE — Retrieve a Task

**Client request:**

```json
{
  "action": "DEQUEUE",
  "timeout": 2
}
```

**Example server response:**

```json
{
  "status": "ok",
  "task": "Complete assignment"
}
```

The queue follows the First-In, First-Out (FIFO) principle.

If no task is available, the server can wait for a task until the specified timeout expires.

If the timeout expires without a task becoming available, the response contains `"task": null`.

### 4.5 Invalid Requests

The server also handles invalid operations and missing required fields.

For example:

```json
{
  "action": "DELETE"
}
```

Produces:

```json
{
  "status": "error",
  "message": "Unknown action"
}
```

---

## 5. Python File Implementation

### 5.1 protocol.py — Message Framing

This file contains the functions responsible for sending and receiving messages.

**Main functions:**

- `recv_exact()` — Reads exactly the required number of bytes.
- `send_message()` — Converts a message into a length-prefixed JSON frame.
- `receive_message()` — Reads, validates, and decodes incoming messages.

**Important concepts:** TCP stream fragmentation, serialization, UTF-8 encoding, binary headers, and network byte order.

### 5.2 storage.py — Shared Data Storage

This file contains the `Storage` class.

It manages two shared data structures:

- A Python dictionary for the key-value store.
- A `deque` for the task queue.

The class provides four methods:

- `put()` — Stores a key-value pair.
- `get()` — Retrieves a value.
- `enqueue()` — Adds a task.
- `dequeue()` — Retrieves a task, waiting when necessary.

The file also uses synchronization primitives to protect shared state.

### 5.3 server.py — Concurrent TCP Server

This file starts the TCP server and handles incoming client connections.

**Main functions:**

- `start_server()` — Creates the listening socket and starts the thread pool.
- `handle_client()` — Receives and responds to client messages.
- `process_request()` — Identifies the requested action and calls the appropriate storage method.

The server uses:

```python
ThreadPoolExecutor(max_workers=10)
```

This limits request-handling execution to a maximum of 10 worker threads.

The server listens on:

```python
HOST = "0.0.0.0"
PORT = 9090
```

### 5.4 client_stress_test.py — Concurrent Client Testing

This file tests the server using 20 concurrent client workers.

Each worker:

1. Opens a TCP connection.
2. Sends a PUT request using a unique key.
3. Checks the server's response.
4. Sends a GET request for the same key.
5. Confirms that the returned value is correct.
6. Reports successful completion.

The test verifies that multiple clients can store and retrieve their own values without observed data corruption or unhandled socket exceptions during the run.

### 5.5 edge_tests.py — Additional Verification

This file contains 10 tests covering normal operations and unusual situations.

It checks how the server behaves when it receives invalid requests, incomplete TCP data, oversized messages, and unexpected client disconnections.

### 5.6 demo_put_get.py — Key-Value Demonstration

This script demonstrates the PUT and GET operations with a simple example.

It stores the value `"Sadaf"` using the key `"student"` and then retrieves it.

### 5.7 demo_queue.py — Task Queue Demonstration

This script demonstrates ENQUEUE and DEQUEUE.

It adds the task `"Complete assignment"` and then retrieves the same task from the queue.

---

## 6. Thread Synchronization and Concurrency

Concurrency is an important part of this project because multiple clients may access the same data at the same time.

Without synchronization, different threads could interfere with shared operations.

### 6.1 threading.Lock

I used `threading.Lock` to protect the shared dictionary and queue.

Only one thread can hold the lock at a time.

This helps prevent race conditions when different worker threads access shared data.

For example, PUT and GET operations acquire the lock while accessing the dictionary.

### 6.2 threading.Condition

I used `threading.Condition` for the task queue.

If a client requests DEQUEUE when the queue is empty, the worker can wait for a new task.

When another client performs ENQUEUE, the condition variable notifies a waiting worker.

This avoids repeatedly checking the queue in a busy loop.

### 6.3 ThreadPoolExecutor

The server uses a thread pool with a maximum of 10 worker threads.

Instead of creating an unlimited number of threads, the server reuses worker threads to process client connections.

This helps control thread usage while supporting concurrent clients.

The stress test uses 20 client workers to verify how the server handles multiple incoming connections.

---

## 7. Testing and Verification

I tested the project using GitHub Codespaces.

The tests included server initialization, port verification, concurrent clients, edge cases, key-value operations, and task queue operations.

### 7.1 Server Initialization

**Command:**

```bash
python3 server.py
```

**Observed output:**

```text
[*] Server initialized on 0.0.0.0:9090
[*] Thread Pool initialized with max_workers=10...
```

The server started successfully and began listening for client connections.

![Server Initialization](docs/screenshots/01_server_running.png)

### 7.2 Port Verification

**Command:**

```bash
lsof -i :9090
```

The output confirmed that a Python process was listening on TCP port 9090.

![Port Verification](docs/screenshots/02_port_verification.png)

### 7.3 Concurrent Stress Test

**Command:**

```bash
python3 client_stress_test.py
```

The test launched 20 concurrent clients.

Each client completed its PUT and GET operations successfully.

**Observed result:**

```text
Stress test completed: 20/20 workers
```

All 20 workers completed successfully in the recorded test run.

![20-Worker Stress Test](docs/screenshots/03_stress_test_20_workers.png)

### 7.4 Edge-Case Testing

**Command:**

```bash
python3 edge_tests.py
```

The edge-case test script executed the following tests:

| # | Test | Result |
|---|---|---|
| 1 | PUT and GET operations | PASS |
| 2 | Nonexistent key | PASS |
| 3 | Unknown action | PASS |
| 4 | Missing request field | PASS |
| 5 | Invalid queue timeout | PASS |
| 6 | FIFO task queue | PASS |
| 7 | Partial TCP reads | PASS |
| 8 | Oversized message rejection | PASS |
| 9 | Invalid JSON handling | PASS |
| 10 | Unexpected client disconnect | PASS |

**Observed result:**

```text
RESULT: 10/10 tests passed
```

These tests showed that the server handled the tested invalid requests and network edge cases without a test failure.

![Edge-Case Test Results](docs/screenshots/04_edge_case_tests.png)

### 7.5 PUT and GET Demonstration

**Command:**

```bash
python3 demo_put_get.py
```

**Observed result:**

```text
========== KEY-VALUE STORE TEST ==========

1. PUT - Store a value
Response: {'status': 'ok'}

2. GET - Retrieve the stored value
Response: {'status': 'ok', 'value': 'Sadaf'}

RESULT: PUT and GET operations PASSED
```

This test confirmed that the server could store a value and retrieve it correctly.

![PUT and GET Operations](docs/screenshots/05_put_get_operations.png)

### 7.6 ENQUEUE and DEQUEUE Demonstration

**Command:**

```bash
python3 demo_queue.py
```

**Observed result:**

```text
========== TASK QUEUE TEST ==========

1. ENQUEUE - Add a task
Response: {'status': 'ok'}

2. DEQUEUE - Retrieve the task
Response: {'status': 'ok', 'task': 'Complete assignment'}

RESULT: ENQUEUE and DEQUEUE operations PASSED
```

This test confirmed that the server could add a task to the queue and retrieve it successfully.

![ENQUEUE and DEQUEUE Operations](docs/screenshots/06_enqueue_dequeue_operations.png)

---

## 8. How to Run the Project

### Requirements

- Python 3
- GitHub Codespaces or another environment with Python socket support
- No external Python packages required

### Step 1 — Open the Repository

Open the project in GitHub Codespaces or another Python development environment.

### Step 2 — Start the Server

In the first terminal:

```bash
python3 server.py
```

Keep this terminal running.

### Step 3 — Open Another Terminal

Use a second terminal to run the tests while the server continues listening.

### Step 4 — Run the Stress Test

```bash
python3 client_stress_test.py
```

### Step 5 — Run the Edge-Case Tests

```bash
python3 edge_tests.py
```

### Step 6 — Run the Demonstrations

```bash
python3 demo_put_get.py
python3 demo_queue.py
```

### Step 7 — Stop the Server

Return to the server terminal and press `Ctrl + C`.

---

## 9. Connection to Distributed Systems Concepts

This assignment helped me understand several important concepts from Distributed Systems.

| Concept | How It Was Used |
|---|---|
| Interprocess Communication | Clients and server exchange messages through TCP sockets |
| Message Framing | Four-byte length headers identify complete messages |
| Marshalling | Python dictionaries are converted into JSON and bytes |
| Concurrency | Multiple clients are handled using a thread pool |
| Mutual Exclusion | Locks protect shared data |
| Condition Variables | Workers wait for tasks without busy waiting |
| Failure Handling | Invalid messages and unexpected disconnects are handled |
| Testing | Concurrent and edge-case tests verify observed behavior |

These concepts relate to topics covered in *Distributed Systems: Concepts and Design*, 5th Edition, by Coulouris, Dollimore, Kindberg, and Blair.

---

## 10. Conclusion

In this project, I successfully implemented a concurrent TCP key-value and task queue server using Python.

The project uses a custom length-prefixed JSON protocol instead of relying on high-level communication frameworks.

I implemented shared storage using a dictionary and a FIFO queue, protected the data using locks and condition variables, and handled multiple clients using a thread pool.

The system passed the 20-worker concurrent stress test and all 10 additional edge-case tests performed during verification.

The most important thing I learned was that TCP communication requires clear message framing rules because TCP does not preserve application message boundaries.

I also learned why synchronization is necessary when multiple threads work with shared data.

Overall, this assignment helped me better understand low-level network communication, concurrency, and synchronization in distributed systems.

---


**COSC 3657 — Distributed Systems | Unit 1**

**Project:** Concurrent TCP Key-Value & Task Queue Server  
**Student:** Sadaf Kabir
 

                    
