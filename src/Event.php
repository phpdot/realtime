<?php

declare(strict_types=1);

/**
 * Event — encode/decode the JSON wire frame {"event":"...","data":...}.
 *
 * Plain JSON text frames over WebSocket. Any client (new WebSocket(...), websocat,
 * raw socket) can speak this — no engine.io, no library lock-in. This envelope is
 * a real-time convention; the transport stays payload-agnostic.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime;

final class Event
{
    /**
     * Encode an event + payload into the JSON wire frame.
     *
     * @param string $event The event name (e.g., 'chat.message').
     * @param mixed $payload The event data (any JSON-serialisable value).
     *
     * @return string
     */
    public static function encode(string $event, mixed $payload = null): string
    {
        return json_encode(['event' => $event, 'data' => $payload], JSON_THROW_ON_ERROR);
    }

    /**
     * Decode a JSON wire frame into event + channel + data + ack.
     *
     * The channel and ack fields are optional (present when the client specifies them).
     *
     * @param string $raw
     *
     * @return array{event: string, channel: string|null, data: mixed, ack: int|null}|null Null if malformed.
     */
    public static function decode(string $raw): ?array
    {
        $decoded = json_decode($raw, true);

        if (!is_array($decoded) || !isset($decoded['event']) || !is_string($decoded['event'])) {
            return null;
        }

        $channel = $decoded['channel'] ?? null;
        $ack = $decoded['ack'] ?? null;

        return [
            'event' => $decoded['event'],
            'channel' => is_string($channel) ? $channel : null,
            'data' => $decoded['data'] ?? null,
            'ack' => is_int($ack) ? $ack : null,
        ];
    }
}
