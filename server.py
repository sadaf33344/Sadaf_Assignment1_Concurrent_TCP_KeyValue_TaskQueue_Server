
import socket
import json
from concurrent.futures import ThreadPoolExecutor

# Import our custom protocol and shared storage.
from protocol import send_message, receive_message
from storage import Storage


# Server settings.
HOST = "0.0.0.0"  # Listen for connections on all network interfaces.
PORT = 9090       # Port used by our TCP server.
MAX_WORKERS = 10  # Maximum number of client-handling threads.

# Create one shared storage object for all connected clients.
storage = Storage()


# Read a client's request and perform the requested operation.
def process_request(request):

    # Make sure the received request is a dictionary.
    if not isinstance(request, dict):
        return {"status": "error", "message": "Invalid request"}

    # Find out which operation the client wants.
    action = request.get("action")

    if action == "PUT":
        # Make sure both the key and value were provided.
        if "key" not in request or "value" not in request:
            return {"status": "error", "message": "Missing key or value"}

        # Save the value using the provided key.
        storage.put(request["key"], request["value"])
        return {"status": "ok"}

    elif action == "GET":
        # A key is required to retrieve a value.
        if "key" not in request:
            return {"status": "error", "message": "Missing key"}

        # Retrieve the value from shared storage.
        value = storage.get(request["key"])
        return {"status": "ok", "value": value}

    elif action == "ENQUEUE":
        # Make sure the client provided a task.
        if "task" not in request:
            return {"status": "error", "message": "Missing task"}

        # Add the task to the shared queue.
        storage.enqueue(request["task"])
        return {"status": "ok"}

    elif action == "DEQUEUE":
        # Wait up to one second by default for a task.
        timeout = request.get("timeout", 1)

        # Only allow timeout values between 0 and 30 seconds.
        if not isinstance(timeout, (int, float)) or not 0 <= timeout <= 30:
            return {"status": "error", "message": "Invalid timeout"}

        # Remove the oldest task, or return None if no task arrives.
        task = storage.dequeue(timeout=timeout)
        return {"status": "ok", "task": task}

    # Return an error if the action is not recognized.
    return {"status": "error", "message": "Unknown action"}


# Handle communication with one connected client.
def handle_client(client_socket, address):
    try:
        # Automatically close the socket when finished.
        with client_socket:

            # Keep receiving requests while the client stays connected.
            while True:
                try:
                    # Receive and decode a request using our protocol.
                    request = receive_message(client_socket)

                except ConnectionError:
                    # Stop when the client closes its connection.
                    break

                except (ValueError, UnicodeError, json.JSONDecodeError) as error:
                    # Send an error response for invalid message data.
                    send_message(
                        client_socket,
                        {"status": "error", "message": str(error)}
                    )
                    break

                # Process the request and send the result to the client.
                response = process_request(request)
                send_message(client_socket, response)

    except (ConnectionError, OSError) as error:
        # Log network errors without stopping the entire server.
        print(f"[!] Client {address} disconnected: {error}")


# Start the TCP server and accept incoming client connections.
def start_server():

    # Create an IPv4 TCP socket.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:

        # Allow the server port to be reused after restarting.
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Connect the socket to the configured address and port.
        server.bind((HOST, PORT))

        # Start listening for incoming client connections.
        server.listen()

        print(f"[*] Server initialized on {HOST}:{PORT}", flush=True)
        print(
            f"[*] Thread Pool initialized with max_workers={MAX_WORKERS}...",
            flush=True
        )

        # Create a pool of up to 10 threads to handle clients.
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            # Keep accepting new clients.
            while True:
                client_socket, address = server.accept()

                # Assign each connected client to the thread pool.
                executor.submit(handle_client, client_socket, address)


# Run the server only when this file is executed directly.
if __name__ == "__main__":
    try:
        start_server()

    except KeyboardInterrupt:
        # Display a message when the server is stopped using Ctrl+C.
        print("\n[*] Server stopped.")
