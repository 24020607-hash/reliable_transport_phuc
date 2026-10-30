import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

HEADER_SIZE = 16
START, END, DATA, ACK = 0, 1, 2, 3


def send_ack(sock, addr, seq):
    """Gửi gói ACK với số thứ tự seq về địa chỉ addr."""
    hdr = PacketHeader(type=ACK, seq_num=seq, length=0)
    hdr.checksum = compute_checksum(hdr)
    sock.sendto(bytes(hdr), addr)


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))
    out = sys.stdout.buffer

    connected = False
    sender_addr = None
    expected = 1        # seq của gói DATA mong đợi tiếp theo
    buffer = {}         # seq -> payload của các gói đến sớm

    while True:
        pkt, addr = s.recvfrom(2048)
        if len(pkt) < HEADER_SIZE:
            continue

        header = PacketHeader(pkt[:HEADER_SIZE])
        payload = pkt[HEADER_SIZE:HEADER_SIZE + header.length]

        # Kiểm tra checksum, sai thì bỏ gói
        received_sum = header.checksum
        header.checksum = 0
        if compute_checksum(bytes(header) + payload) != received_sum:
            continue

        if connected and addr != sender_addr:
            continue

        if header.type == START:
            if connected:
                continue
            connected = True
            sender_addr = addr
            expected = 1
            buffer = {}
            send_ack(s, addr, 1)

        elif header.type == DATA:
            if not connected:
                continue
            seq = header.seq_num
            if seq >= expected + window_size:
                continue                    # ngoài cửa sổ: bỏ
            if seq >= expected and seq not in buffer:
                buffer[seq] = payload
            while expected in buffer:       # ghi các gói liên tiếp ra
                out.write(buffer.pop(expected))
                expected += 1
            send_ack(s, addr, seq)          # ACK riêng đúng gói vừa nhận

        elif header.type == END:
            if not connected:
                continue
            if header.seq_num != expected:
                continue
            send_ack(s, addr, header.seq_num)
            break

    out.flush()


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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
