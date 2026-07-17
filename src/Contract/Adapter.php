<?php

declare(strict_types=1);

/**
 * Pluggable membership + broadcast + presence + identity store.
 *
 * Two implementations:
 * - TableAdapter: Swoole\Table (dev / single-instance, cross-worker shared memory).
 * - RedisAdapter: Redis pub/sub + HASH/SET (production / multi-instance).
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Contract;

interface Adapter
{
    /**
     * Add an fd to room(s), optionally recording user identity.
     *
     * @param list<string> $rooms Room names.
     * @param array<string, mixed>|null $user User identity {id, name, ...} (for presence).
     * @param int $fd
     *
     * @return void
     */
    public function add(int $fd, array $rooms, ?array $user = null): void;

    /**
     * Remove an fd from room(s).
     *
     * @param list<string> $rooms Room names.
     * @param int $fd
     *
     * @return void
     */
    public function del(int $fd, array $rooms): void;

    /**
     * Remove an fd from ALL rooms + identity maps (disconnect cleanup).
     *
     * @param int $fd
     *
     * @return void
     */
    public function delAll(int $fd): void;

    /**
     * Broadcast a JSON frame to all fds in the target rooms, excluding exceptFds.
     *
     * @param string $jsonFrame The encoded event: {"event":"...","data":...}
     * @param list<string> $rooms Target rooms (empty = all connections).
     * @param list<int> $exceptFds Fds to exclude.
     *
     * @return void
     */
    public function broadcast(string $jsonFrame, array $rooms, array $exceptFds): void;

    /**
     * Send a JSON frame to ONE fd (direct/private message).
     *
     * @param int $fd
     * @param string $jsonFrame
     *
     * @return void
     */
    public function send(int $fd, string $jsonFrame): void;

    /**
     * List members in room(s).
     *
     * @param list<string> $rooms Room names.
     *
     * @return list<array{fd: int, user: array<mixed, mixed>|null}>
     */
    public function members(array $rooms): array;

    /**
     * Count members in room(s).
     *
     * @param list<string> $rooms Room names.
     *
     * @return int
     */
    public function count(array $rooms): int;

    /**
     * Get the user identity for an fd (null if unknown or not authenticated).
     *
     * @param int $fd
     *
     * @return array<mixed, mixed>|null
     */
    public function userOf(int $fd): ?array;

    /**
     * Get all fds belonging to a userId.
     *
     * @param string $userId
     *
     * @return list<int>
     */
    public function fdsOfUser(string $userId): array;

    /**
     * Get the rooms an fd belongs to.
     *
     * @param int $fd
     *
     * @return list<string>
     */
    public function roomsOf(int $fd): array;

    /**
     * Force-disconnect EVERY connection belonging to a userId, across the whole cluster (all
     * nodes, all workers) — e.g. on logout / session revocation. Single-node adapters disconnect
     * locally; multi-node adapters relay the revoke so a user's connections on other nodes are
     * dropped too. Each disconnected socket unwinds through the transport's normal close path.
     *
     * @param string $userId
     *
     * @return void
     */
    public function disconnectUser(string $userId): void;
}
