<?php

declare(strict_types=1);

/**
 * The realtime adapter's Redis commands over a phpdot/redis connection.
 *
 * The adapter names the {@see RedisCommands} contract so it never cares which
 * client a host runs; this bridge is the answer when that client is
 * phpdot/redis. The connection arrives as a BORROW per call — the adapter is
 * a singleton shared by every coroutine, and one held from its birth would
 * never go back to the pool:
 *
 *     RedisCommands::class => singleton(
 *         fn (ContainerInterface $c): RedisCommands => new RedisConnectionCommands(
 *             connection: fn (): RedisConnection => $c->get(RedisConnection::class),
 *         ),
 *     );
 *
 * The bridge is optional by package position — phpdot/redis sits in
 * require-dev + suggest — and nothing outside Bridge/ names it.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Bridge;

use Closure;
use PHPdot\Realtime\Contract\RedisCommands;
use PHPdot\Redis\RedisConnection;

final readonly class RedisConnectionCommands implements RedisCommands
{
    /**
     * @param Closure(): RedisConnection $connection Answers the borrowed connection for one call
     */
    public function __construct(private Closure $connection) {}

    public function hSet(string $key, string $field, string $value): void
    {
        $this->redis()->hSet($key, $field, $value);
    }

    public function hDel(string $key, string $field): void
    {
        $this->redis()->hDel($key, $field);
    }

    public function hGet(string $key, string $field): null|string
    {
        $found = $this->redis()->hGet($key, $field);

        return is_string($found) ? $found : null;
    }

    /**
     * @return array<string, string>
     */
    public function hGetAll(string $key): array
    {
        $clean = [];

        foreach ($this->redis()->hGetAll($key) as $field => $value) {
            $clean[(string) $field] = $value;
        }

        return $clean;
    }

    public function hLen(string $key): int
    {
        $count = $this->redis()->hLen($key);

        return is_int($count) ? $count : 0;
    }

    public function sAdd(string $key, string $member): void
    {
        $this->redis()->sAdd($key, $member);
    }

    public function sRem(string $key, string $member): void
    {
        $this->redis()->srem($key, $member);
    }

    /**
     * @return list<string>
     */
    public function sMembers(string $key): array
    {
        $rows = $this->redis()->sMembers($key);

        return is_array($rows) ? array_values(array_filter($rows, is_string(...))) : [];
    }

    public function del(string $key): void
    {
        $this->redis()->del($key);
    }

    public function publish(string $channel, string $message): void
    {
        $this->redis()->publish($channel, $message);
    }

    public function setEx(string $key, string $value, int $ttlSeconds): void
    {
        $this->redis()->setex($key, $ttlSeconds, $value);
    }

    public function setNx(string $key, string $value, int $ttlSeconds): bool
    {
        return $this->redis()->set($key, $value, ['NX', 'EX' => $ttlSeconds]) !== false;
    }

    public function exists(string $key): bool
    {
        return $this->redis()->exists($key) > 0;
    }

    public function get(string $key): null|string
    {
        $found = $this->redis()->get($key);

        return is_string($found) ? $found : null;
    }

    /**
     * The borrowed client for one call, connected when the borrow answers one
     * that is not — a raw construction (no pool) lands here unconnected, and
     * the adapter's first command must not be the one that discovers it.
     *
     * @return \Redis
     */
    private function redis(): \Redis
    {
        $connection = ($this->connection)();

        if (!$connection->isConnected()) {
            $connection->connect();
        }

        return $connection->getClient();
    }
}
