
import json
import struct

# Read exactly the number of bytes we need from the socket.
# TCP may send data in smaller pieces, so we keep reading until we have everything.
def recv_exact(sock, n):
    data = bytearray()

    # Continue receiving until we have n bytes.
    while len(data) < n:
        chunk = sock.recv(n - len(data))

        # If no data arrives, the connection has been closed.
        if not chunk:
            raise ConnectionError("Connection closed before all bytes arrived")

        # Add the received bytes to our data.
        data.extend(chunk)

    return bytes(data)


# Convert a Python dictionary into JSON and send it to the client/server.
def send_message(sock, message):
    # Convert the message into JSON bytes.
    payload = json.dumps(message).encode("utf-8")

    # Create a 4-byte header containing the message length.
    # !I means an unsigned integer in big-endian format.
    header = struct.pack("!I", len(payload))

    # Send the header and the actual message together.
    sock.sendall(header + payload)


# Receive a message from the socket and convert it back into Python data.
def receive_message(sock):
    # First, receive the 4-byte header.
    header = recv_exact(sock, 4)

    # Find out how many bytes the JSON message contains.
    message_length = struct.unpack("!I", header)[0]

    # Reject empty messages or messages larger than 1 MB.
    if message_length == 0 or message_length > 1024 * 1024:
        raise ValueError("Invalid message length")

    # Receive the complete JSON message.
    payload = recv_exact(sock, message_length)

    # Convert JSON bytes back into a Python object.
    return json.loads(payload.decode("utf-8"))
