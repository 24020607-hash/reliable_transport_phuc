import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16
MAX_PAYLOAD = 1472 - HEADER_SIZE   # tổng gói tối đa 1472 byte
TIMEOUT = 0.5                      # 500 ms
START, END, DATA, ACK = 0, 1, 2, 3


def build_packet(ptype, seq, payload=b""):
    """Dựng gói: header (đã có checksum) + dữ liệu."""
    hdr = PacketHeader(type=ptype, seq_num=seq, length=len(payload))
    hdr.checksum = compute_checksum(bytes(hdr) + payload)
    return bytes(hdr) + payload


def parse_ack(pkt):
    """Trả về seq_num nếu là ACK hợp lệ, ngược lại trả về None."""
    if len(pkt) < HEADER_SIZE:
        return None
    hdr = PacketHeader(pkt[:HEADER_SIZE])
    received = hdr.checksum
    hdr.checksum = 0
    if compute_checksum(bytes(hdr)) != received or hdr.type != ACK:
        return None
    return hdr.seq_num


def sender(receiver_ip, receiver_port, window_size):
    data = sys.stdin.buffer.read()
    chunks = [data[i:i + MAX_PAYLOAD] for i in range(0, len(data), MAX_PAYLOAD)]
    n = len(chunks)                       # số gói DATA, seq từ 1 đến n
    packets = {seq: build_packet(DATA, seq, chunks[seq - 1])
               for seq in range(1, n + 1)}

    addr = (receiver_ip, receiver_port)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # 1. Gửi START (seq 0) và chờ ACK seq 1
    start_pkt = build_packet(START, 0)
    while True:
        s.sendto(start_pkt, addr)
        s.settimeout(TIMEOUT)
        try:
            pkt, _ = s.recvfrom(2048)
        except socket.timeout:
            continue
        if parse_ack(pkt) == 1:
            break

    # 2. Gửi DATA bằng cửa sổ trượt
    base = 1          # gói cũ nhất chưa được ACK
    next_seq = 1      # gói tiếp theo sẽ gửi
    deadline = time.time() + TIMEOUT

    while base <= n:
        # Gửi thêm gói khi cửa sổ còn chỗ
        while next_seq < base + window_size and next_seq <= n:
            s.sendto(packets[next_seq], addr)
            next_seq += 1

        remaining = deadline - time.time()
        if remaining <= 0:
            # Hết giờ mà cửa sổ chưa trượt: gửi lại cả cửa sổ
            for seq in range(base, next_seq):
                s.sendto(packets[seq], addr)
            deadline = time.time() + TIMEOUT
            continue

        s.settimeout(remaining)
        try:
            pkt, _ = s.recvfrom(2048)
        except socket.timeout:
            continue

        ack = parse_ack(pkt)
        if ack is not None and ack > base:
            base = min(ack, next_seq)      # cửa sổ trượt tiến lên
            deadline = time.time() + TIMEOUT

    # 3. Gửi END (seq n+1), chờ ACK tối đa 500 ms
    s.sendto(build_packet(END, n + 1), addr)
    end_deadline = time.time() + TIMEOUT
    while True:
        remaining = end_deadline - time.time()
        if remaining <= 0:
            break
        s.settimeout(remaining)
        try:
            pkt, _ = s.recvfrom(2048)
        except socket.timeout:
            break
        if parse_ack(pkt) == n + 1:
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
