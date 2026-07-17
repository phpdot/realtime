<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Support;

use PHPdot\Contracts\Server\ConnectionSenderInterface;
use PHPdot\Realtime\Event;

/**
 * In-memory ConnectionSenderInterface for testing the Hub with no server present.
 * Records every frame pushed per fd and every disconnect call.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class FakeSender implements ConnectionSenderInterface
{
    /** @var array<int, list<string>> fd → raw frames pushed. */
    public array $sent = [];

    /** @var list<array{fd: int, code: int, reason: string}> */
    public array $disconnected = [];

    /** @var list<int> fds sent a PING. */
    public array $pinged = [];

    public function pushWs(int $fd, string $frame): bool
    {
        $this->sent[$fd][] = $frame;

        return true;
    }

    public function pingWs(int $fd): bool
    {
        $this->pinged[] = $fd;

        return true;
    }

    public function disconnect(int $fd, int $code = 1000, string $reason = ''): bool
    {
        $this->disconnected[] = ['fd' => $fd, 'code' => $code, 'reason' => $reason];

        return true;
    }

    public function exists(int $fd): bool
    {
        return true;
    }

    /**
     * Decoded events delivered to $fd, in order.
     *
     * @return list<array{event: string, channel: string|null, data: mixed, ack: int|null}>
     */
    public function eventsTo(int $fd): array
    {
        $events = [];
        foreach ($this->sent[$fd] ?? [] as $frame) {
            $decoded = Event::decode($frame);
            if ($decoded !== null) {
                $events[] = $decoded;
            }
        }

        return $events;
    }

    /**
     * The event names delivered to $fd, in order.
     *
     * @return list<string>
     */
    public function eventNamesTo(int $fd): array
    {
        return array_map(static fn(array $e): string => $e['event'], $this->eventsTo($fd));
    }
}
