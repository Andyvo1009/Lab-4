import socket
import time

def start_server(host='localhost', port=6100):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reuse of address
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Socket server listening on {host}:{port}")

    conn, addr = server_socket.accept()
    print(f"Connected by {addr}")

    try:
        while True:
            message = f"Hello Spark! {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            conn.sendall(message.encode('utf-8'))  # Send with \n
            print(f"Sent: {message.strip()}")
              # Every 2 seconds
    except BrokenPipeError:
        print("Client disconnected.")
    finally:
        conn.close()
        server_socket.close()

if __name__ == "__main__":
    start_server()
