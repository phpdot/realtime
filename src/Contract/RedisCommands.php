<?php

declare(strict_types=1);

/**
 * RedisCommands — the minimal Redis command surface the RedisAdapter needs, so the
 * realtime package stays decoupled from any concrete Redis client (ext-redis, Predis,
 * phpdot/redis). The consuming app supplies an implementation (dot wraps phpdot/redis).
 *
 * Each call must run against a coroutine-safe connection (borrow-per-coroutine under
 * Swoole) — the adapter is handed a provider that yields one. This is the COMMAND
 * channel only; the blocking SUBSCRIBE loop lives in RedisSubscriber on its own
 * dedicated connection (pub/sub monopolises a socket).
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Contract;

interface RedisCommands
{
    /**
     * Set a HASH field.
     *
     * @param string $key
     * @param string $field
     * @param string $value
     *
     * @return void
     */
    public function hSet(string $key, string $field, string $value): void;

    /**
     * Delete a HASH field.
     *
     * @param string $key
     * @param string $field
     *
     * @return void
     */
    public function hDel(string $key, string $field): void;

    /**
     * Get one HASH field, or null if absent.
     *
     * @param string $key
     * @param string $field
     *
     * @return ?string
     */
    public function hGet(string $key, string $field): ?string;

    /**
     * Get every field/value of a HASH.
     *
     * @param string $key
     *
     * @return array<string, string>
     */
    public function hGetAll(string $key): array;

    /**
     * Number of fields in a HASH.
     *
     * @param string $key
     *
     * @return int
     */
    public function hLen(string $key): int;

    /**
     * Add a member to a SET.
     *
     * @param string $member
     * @param string $key
     *
     * @return void
     */
    public function sAdd(string $key, string $member): void;

    /**
     * Remove a member from a SET.
     *
     * @param string $key
     * @param string $member
     *
     * @return void
     */
    public function sRem(string $key, string $member): void;

    /**
     * All members of a SET.
     *
     * @param string $key
     *
     * @return list<string>
     */
    public function sMembers(string $key): array;

    /**
     * Delete a key.
     *
     * @param string $key
     *
     * @return void
     */
    public function del(string $key): void;

    /**
     * Publish a message to a channel (broadcast relay).
     *
     * @param string $channel
     * @param string $message
     *
     * @return void
     */
    public function publish(string $channel, string $message): void;

    /**
     * Set a string key with a TTL in seconds (node-liveness heartbeat).
     *
     * @param int $ttlSeconds
     * @param string $key
     * @param string $value
     *
     * @return void
     */
    public function setEx(string $key, string $value, int $ttlSeconds): void;

    /**
     * Set a string key with a TTL only if it does not already exist (SET NX EX).
     * Returns true if the key was set — used as a single-winner reap lock.
     *
     * @param string $key
     * @param string $value
     * @param int $ttlSeconds
     *
     * @return bool
     */
    public function setNx(string $key, string $value, int $ttlSeconds): bool;

    /**
     * Whether a key exists (node-liveness check).
     *
     * @param string $key
     *
     * @return bool
     */
    public function exists(string $key): bool;

    /**
     * Get a string key's value, or null if absent (reading a node's stats snapshot).
     *
     * @param string $key
     *
     * @return ?string
     */
    public function get(string $key): ?string;
}
