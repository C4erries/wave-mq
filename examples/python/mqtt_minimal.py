"""Minimal MQTT 3.1.1 client helpers for wave-mq examples.

No third-party dependencies are required.
"""

from __future__ import annotations

import socket
import struct
from dataclasses import dataclass
from typing import Optional


class MQTTError(RuntimeError):
    """Raised for MQTT protocol-level errors."""


@dataclass
class PublishMessage:
    topic: str
    payload: bytes
    qos: int
    packet_id: Optional[int]


def _encode_utf8(value: str) -> bytes:
    data = value.encode("utf-8")
    return struct.pack("!H", len(data)) + data


def _encode_remaining_length(length: int) -> bytes:
    if length < 0:
        raise ValueError("remaining length must be >= 0")
    encoded = bytearray()
    while True:
        digit = length % 128
        length //= 128
        if length > 0:
            digit |= 0x80
        encoded.append(digit)
        if length == 0:
            return bytes(encoded)


def _read_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise MQTTError("connection closed")
        chunks.extend(chunk)
    return bytes(chunks)


def _read_packet(sock: socket.socket) -> tuple[int, bytes]:
    fixed_header = _read_exact(sock, 1)[0]
    multiplier = 1
    remaining = 0
    for _ in range(4):
        digit = _read_exact(sock, 1)[0]
        remaining += (digit & 0x7F) * multiplier
        if (digit & 0x80) == 0:
            payload = _read_exact(sock, remaining)
            return fixed_header, payload
        multiplier *= 128
    raise MQTTError("malformed remaining length")


def _write_packet(sock: socket.socket, packet_type: int, flags: int, payload: bytes) -> None:
    header = bytes([((packet_type & 0x0F) << 4) | (flags & 0x0F)])
    sock.sendall(header + _encode_remaining_length(len(payload)) + payload)


def connect(
    host: str,
    port: int,
    client_id: str,
    *,
    clean_session: bool = True,
    keep_alive_sec: int = 30,
    timeout_sec: float = 5.0,
) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=timeout_sec)
    sock.settimeout(timeout_sec)

    connect_flags = 0x02 if clean_session else 0x00
    variable_header = _encode_utf8("MQTT") + bytes([0x04, connect_flags]) + struct.pack("!H", keep_alive_sec)
    payload = _encode_utf8(client_id)
    _write_packet(sock, packet_type=1, flags=0, payload=variable_header + payload)

    first, packet = _read_packet(sock)
    if (first >> 4) != 2:
        raise MQTTError(f"expected CONNACK, got packet type {first >> 4}")
    if len(packet) != 2:
        raise MQTTError("invalid CONNACK payload")
    return_code = packet[1]
    if return_code != 0:
        raise MQTTError(f"broker rejected CONNECT (code={return_code})")
    sock.settimeout(None)
    return sock


def subscribe(sock: socket.socket, topic: str, *, qos: int = 0, packet_id: int = 1) -> None:
    if qos not in (0, 1):
        raise ValueError("qos must be 0 or 1")
    payload = struct.pack("!H", packet_id) + _encode_utf8(topic) + bytes([qos])
    _write_packet(sock, packet_type=8, flags=0x02, payload=payload)

    first, packet = _read_packet(sock)
    if (first >> 4) != 9:
        raise MQTTError(f"expected SUBACK, got packet type {first >> 4}")
    if len(packet) < 3:
        raise MQTTError("invalid SUBACK payload")
    ack_packet_id = struct.unpack("!H", packet[:2])[0]
    if ack_packet_id != packet_id:
        raise MQTTError(f"unexpected SUBACK packet id {ack_packet_id}")
    if packet[2] == 0x80:
        raise MQTTError("broker rejected SUBSCRIBE")


def publish(
    sock: socket.socket,
    topic: str,
    payload: bytes | str,
    *,
    qos: int = 0,
    packet_id: int = 1,
) -> None:
    if qos not in (0, 1):
        raise ValueError("qos must be 0 or 1")
    body = payload.encode("utf-8") if isinstance(payload, str) else payload
    variable_header = _encode_utf8(topic)
    if qos == 1:
        variable_header += struct.pack("!H", packet_id)
    _write_packet(sock, packet_type=3, flags=(qos << 1), payload=variable_header + body)

    if qos == 1:
        first, packet = _read_packet(sock)
        if (first >> 4) != 4:
            raise MQTTError(f"expected PUBACK, got packet type {first >> 4}")
        if len(packet) != 2:
            raise MQTTError("invalid PUBACK payload")
        ack_packet_id = struct.unpack("!H", packet)[0]
        if ack_packet_id != packet_id:
            raise MQTTError(f"unexpected PUBACK packet id {ack_packet_id}")


def recv_publish(sock: socket.socket, *, timeout_sec: Optional[float] = None) -> PublishMessage:
    previous_timeout = sock.gettimeout()
    sock.settimeout(timeout_sec)
    try:
        while True:
            first, packet = _read_packet(sock)
            packet_type = first >> 4
            flags = first & 0x0F
            if packet_type != 3:
                continue
            if len(packet) < 2:
                raise MQTTError("invalid PUBLISH packet")

            topic_len = struct.unpack("!H", packet[:2])[0]
            idx = 2
            topic_end = idx + topic_len
            if topic_end > len(packet):
                raise MQTTError("invalid PUBLISH topic length")
            topic = packet[idx:topic_end].decode("utf-8")
            idx = topic_end

            qos = (flags >> 1) & 0x03
            packet_id = None
            if qos > 0:
                if idx+2 > len(packet):
                    raise MQTTError("invalid PUBLISH packet id")
                packet_id = struct.unpack("!H", packet[idx:idx+2])[0]
                idx += 2

            payload = packet[idx:]
            if qos == 1 and packet_id is not None:
                _write_packet(sock, packet_type=4, flags=0, payload=struct.pack("!H", packet_id))
            return PublishMessage(topic=topic, payload=payload, qos=qos, packet_id=packet_id)
    finally:
        sock.settimeout(previous_timeout)


def disconnect(sock: socket.socket) -> None:
    try:
        _write_packet(sock, packet_type=14, flags=0, payload=b"")
    finally:
        sock.close()
