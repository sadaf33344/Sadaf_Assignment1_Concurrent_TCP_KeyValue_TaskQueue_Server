
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import functions from our custom TCP protocol.
from protocol import send_message, receive_message


# Connection settings for the local server.
HOST = "127.0.0.1"
PORT = 9090

# Number of clients we want to test at the same time.
NUM_WORKERS = 20


# Each worker acts like a separate client connecting to the server.
def worker(worker_id):

    # Connect to the TCP server.
    # A timeout prevents the client from waiting forever.
    with socket.create_connection((HOST, PORT), timeout=15) as sock:
        sock.settimeout(15)

        # Give each worker its own key and value.
        key = f"worker_{worker_id}"
        value = f"value_{worker_id}"

        # Send a PUT request to store the worker's value.
        send_message(sock, {
            "action": "PUT",
            "key": key,
            "value": value
        })

        # Check that the server successfully stored the value.
        response = receive_message(sock)
        assert response["status"] == "ok"

        # Send a GET request to retrieve the stored value.
        send_message(sock, {
            "action": "GET",
            "key": key
        })

        # Check that the server returned the correct value.
        response = receive_message(sock)
        assert response["status"] == "ok"
        assert response["value"] == value

        # Return the worker ID when both operations succeed.
        return worker_id


# Start the stress test and collect results from all workers.
def main():
    print("Starting stress test with 20 concurrent workers...")

    completed = 0

    # Create a pool of 20 client threads.
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:

        # Start all 20 workers.
        futures = [
            executor.submit(worker, i)
            for i in range(NUM_WORKERS)
        ]

        # Process the results as each worker finishes.
        for future in as_completed(futures):

            # Raise an error if a worker failed.
            worker_id = future.result()

            completed += 1
            print(f"Worker {worker_id}: completed successfully")

    # Show the final number of successful workers.
    print(f"\nStress test completed: {completed}/{NUM_WORKERS} workers")

    # Confirm that every worker completed successfully.
    assert completed == NUM_WORKERS


# Run the test when this file is executed directly.
if __name__ == "__main__":
    main()
