<?php

declare(strict_types=1);

/**
 * RoomFacade — presence queries for a room. Returned by Hub::room($name).
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime;

use PHPdot\Realtime\Contract\Adapter;

final class RoomFacade
{
    /**
     * A fluent facade scoped to a single room.
     *
     * @param Adapter $adapter Room/presence backend.
     * @param string $room The room name this facade is scoped to.
     */
    public function __construct(
        private readonly Adapter $adapter,
        private readonly string $room,
    ) {}

    /**
     * List all members with their identity.
     *
     * @return list<array{fd: int, user: array<int|string, mixed>|null}>
     */
    public function members(): array
    {
        return $this->adapter->members([$this->room]);
    }

    /**
     * Count of members in this room.
     *
     * @return int
     */
    public function count(): int
    {
        return $this->adapter->count([$this->room]);
    }
}
