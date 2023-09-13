import socket

# Tạo một socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Gắn máy chủ với địa chỉ và cổng (localhost và cổng 9994)
server_socket.bind(("localhost", 9994))

# Lắng nghe kết nối
server_socket.listen(5)
print("Server is listening on port 9994")

while True:
    # Chấp nhận kết nối từ client
    client_socket, address = server_socket.accept()
    print(f"Received connection from {address}")

    # Gửi dữ liệu đến client (ví dụ: "Hello, Spark Streaming!")
    message = "Hello, Spark Streaming!"
    client_socket.send(message.encode())

    # Đóng kết nối
    client_socket.close()
