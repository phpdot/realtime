<?php

declare(strict_types=1);

/**
 * BroadcastOperator — chainable targeting builder. Returned by Hub::to()/except()/direct()
 * and Socket::broadcast()/to()/except()/direct(). Accumulates rooms + exclusions, then
 * emit() resolves targets and dispatches via the adapter.
 *
 * Targeting is EXPLICIT (SignalR-style):
 * - to(room) is LITERAL — the full room, including the sender if they're a member.
 * - broadcast() (on Socket) seeds except($socket->id()) — the ONLY place "exclude me" lives.
 * - except(fd) adds an explicit exclusion.
 * - direct(fd) targets ONE socket (bypasses rooms).
 *
 * Methods ACCUMULATE: to('a')->to('b') → rooms {a, b}. except($fd1)->except($fd2) → {fd1, fd2}.
 * emit(event, payload) is always the terminal — constructs {"event":"...","data":...} and dispatches.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime;

use PHPdot\Realtime\Contract\Adapter;

final class BroadcastOperator
{
    /**
     * @var list<string> accumulated target rooms (union).
     */
    private array $rooms = [];

    /**
     * @var list<int> accumulated fd exclusions.
     */
    private array $exceptFds = [];

    /**
     * Direct target fd (set by direct(); bypasses rooms).
     */
    private int|null $targetFd = null;

    /**
     * A fluent operator that targets rooms or sockets and emits an event to them.
     *
     * @param Adapter $adapter The adapter to dispatch through.
     */
    public function __construct(
        private readonly Adapter $adapter,
    ) {}

    /**
     * ADD target room(s) — accumulates (union of all to() calls).
     *
     * @param string ...$rooms Room names.
     *
     * @return static
     */
    public function to(string ...$rooms): static
    {
        foreach ($rooms as $room) {
            $this->rooms[] = $room;
        }

        return $this;
    }

    /**
     * Alias of to().
     *
     * @param string $rooms
     *
     * @return static
     */
    public function in(string ...$rooms): static
    {
        return $this->to(...$rooms);
    }

    /**
     * ADD exclusion(s) — accumulates.
     *
     * @param int ...$fds File descriptors to exclude.
     *
     * @return static
     */
    public function except(int ...$fds): static
    {
        foreach ($fds as $fd) {
            $this->exceptFds[] = $fd;
        }

        return $this;
    }

    /**
     * Terminal: encode the JSON frame, resolve targets, dispatch via adapter.
     *
     * @param string $event The event name (e.g., 'chat.message').
     * @param mixed $payload The event data.
     *
     * @return void
     */
    public function emit(string $event, mixed $payload = null): void
    {
        $frame = Event::encode($event, $payload);

        if ($this->targetFd !== null) {
            $this->adapter->send($this->targetFd, $frame);

            return;
        }

        $this->adapter->broadcast($frame, $this->rooms, $this->exceptFds);
    }

    /**
     * Set the direct target fd (bypasses rooms). Used by Hub::direct() / Socket::direct().
     *
     * @param int $fd
     *
     * @return static
     */
    public function setTargetFd(int $fd): static
    {
        $this->targetFd = $fd;

        return $this;
    }

    /**
     * Seed an exclusion (used by Socket::broadcast() to exclude self).
     *
     * @param int $fd
     *
     * @return static
     */
    public function addExcept(int $fd): static
    {
        $this->exceptFds[] = $fd;

        return $this;
    }
}
