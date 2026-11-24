"""Silently drop TCP input on one controlled socket and observe the wire."""
import ctypes
import json
import select
import socket
import struct
import sys
import time

SO_ATTACH_FILTER = 26
BPF_RETURN_CONSTANT = 0x06
ETH_P_ALL = 0x0003
ETH_P_IP = 0x0800

class SockFilter(ctypes.Structure):
    _fields_ = [
        ("code", ctypes.c_ushort),
        ("jump_true", ctypes.c_ubyte),
        ("jump_false", ctypes.c_ubyte),
        ("argument", ctypes.c_uint32),
    ]

class SockFprog(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_ushort),
        ("filter", ctypes.POINTER(SockFilter)),
    ]

def blackhole_input(sock):
    instruction = SockFilter(BPF_RETURN_CONSTANT, 0, 0, 0)
    program = SockFprog(1, ctypes.pointer(instruction))
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.setsockopt(
        sock.fileno(), socket.SOL_SOCKET, SO_ATTACH_FILTER,
        ctypes.byref(program), ctypes.sizeof(program),
    ):
        raise OSError(ctypes.get_errno(), "SO_ATTACH_FILTER failed")

def read_until(sock, marker):
    data = bytearray()
    while marker not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError(f"peer closed before {marker!r}")
        data.extend(chunk)
        if len(data) > 65536:
            raise RuntimeError(f"too much login data before {marker!r}")

def watch_wire(trace, interface, local, remote):
    inbound = peer_ack = peer_reset_or_finish = 0
    while True:
        readable, _, _ = select.select([trace, sys.stdin], [], [])
        if trace in readable:
            packet, link = trace.recvfrom(65535)
            # ETH_P_ALL observes the outgoing taps as well as incoming packets.
            # Loopback captures the same packet twice, once at each tap.
            duplicates = interface == "lo" and link[2] == socket.PACKET_OUTGOING
            ipv4_tcp = (
                link[1] == ETH_P_IP and len(packet) >= 40
                and packet[0] >> 4 == 4 and packet[9] == socket.IPPROTO_TCP
            )
            if not duplicates and ipv4_tcp:
                tcp_offset = (packet[0] & 0x0F) * 4
                if 20 <= tcp_offset and len(packet) >= tcp_offset + 20:
                    source_port, dest_port = struct.unpack_from("!HH", packet, tcp_offset)
                    source = (packet[12:16], source_port)
                    dest = (packet[16:20], dest_port)
                    flags = packet[tcp_offset + 13]
                    if source == remote and dest == local:
                        inbound += 1
                    elif source == local and dest == remote:
                        peer_ack += bool(flags & 0x10)
                        peer_reset_or_finish += bool(flags & 0x05)
        if sys.stdin in readable:
            command = sys.stdin.readline()
            if command == "report\n":
                print(json.dumps({
                    "phase": "report", "inbound": inbound, "peer_ack": peer_ack,
                    "peer_reset_or_finish": peer_reset_or_finish,
                }), flush=True)
                return
            if not command:
                return
            raise RuntimeError(f"unexpected command {command!r}")

def acquire_peer(host, port):
    if port:
        peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer.settimeout(5)
        peer.connect((host, port))
        return peer
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.settimeout(5)
        listener.bind((host, 0))
        listener.listen(1)
        print(json.dumps({"phase": "listening", "port": listener.getsockname()[1]}), flush=True)
        peer, _ = listener.accept()
        peer.settimeout(5)
        return peer

def main():
    host, port, interface = sys.argv[1:]
    with acquire_peer(host, int(port)) as peer:
        read_until(peer, b"login: ")
        peer.sendall(b"K1ABC-1\r\n")
        read_until(peer, b"READY\r\n")
        # Complete normal login traffic before arming a wire observation.
        time.sleep(0.2)
        print('{"phase":"logged in"}', flush=True)
        if sys.stdin.readline() != "drop\n":
            raise RuntimeError("server did not request a packet blackhole")
        local_ip, local_port = peer.getsockname()
        remote_ip, remote_port = peer.getpeername()
        local = (socket.inet_aton(local_ip), local_port)
        remote = (socket.inet_aton(remote_ip), remote_port)
        with socket.socket(socket.AF_PACKET, socket.SOCK_DGRAM, socket.htons(ETH_P_ALL)) as trace:
            trace.bind((interface, ETH_P_ALL))
            blackhole_input(peer)
            print('{"phase":"dropping"}', flush=True)
            watch_wire(trace, interface, local, remote)

if __name__ == "__main__":
    main()
