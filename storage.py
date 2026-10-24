
import threading
from collections import deque


# This class stores key-value data and manages a task queue.
# Locks help prevent problems when multiple clients access shared data.
class Storage:

    def __init__(self):
        # Dictionary used to store key-value pairs.
        self.data = {}

        # Queue used to store tasks in first-in, first-out (FIFO) order.
        self.queue = deque()

        # Only one thread can access protected data at a time.
        self.lock = threading.Lock()

        # Allows threads to wait until a task becomes available.
        # The condition uses the same lock for synchronization.
        self.condition = threading.Condition(self.lock)

    def put(self, key, value):
        """Store a value using its key."""

        # Lock the shared dictionary while updating it.
        with self.lock:
            self.data[key] = value

    def get(self, key):
        """Retrieve a value by key."""

        # Lock the dictionary while reading from it.
        with self.lock:
            return self.data.get(key)

    def enqueue(self, task):
        """Add a task to the queue."""

        # Lock the queue before adding a new task.
        with self.condition:
            self.queue.append(task)

            # Wake up one waiting thread because a task is available.
            self.condition.notify()

    def dequeue(self, timeout=None):
        """Remove the oldest task, waiting if necessary."""

        # Lock the queue while checking and removing tasks.
        with self.condition:

            # Wait until the queue has a task or the timeout expires.
            ready = self.condition.wait_for(
                lambda: len(self.queue) > 0,
                timeout=timeout
            )

            # Return None if no task arrived before the timeout.
            if not ready:
                return None

            # Remove and return the oldest task (FIFO).
            return self.queue.popleft()
