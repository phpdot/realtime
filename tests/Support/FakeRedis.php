<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Support;

use PHPdot\Realtime\Contract\RedisCommands;

/**
 * In-memory RedisCommands — HASHes, SETs, and a recorded publish log, so RedisAdapter
 * can be unit-tested without a real Redis server.
 */
final class FakeRedis implements RedisCommands
{
    /** @var array<string, array<string, string>> */
    public array $hashes = [];

    /** @var array<string, list<string>> */
    public array $sets = [];

    /** @var array<string, string> */
    public array $strings = [];

    /** @var list<array{channel: string, message: string}> */
    public array $published = [];

    public function hSet(string $key, string $field, string $value): void
    {
        $this->hashes[$key][$field] = $value;
    }

    public function hDel(string $key, string $field): void
    {
        unset($this->hashes[$key][$field]);
    }

    public function hGet(string $key, string $field): ?string
    {
        return $this->hashes[$key][$field] ?? null;
    }

    public function hGetAll(string $key): array
    {
        return $this->hashes[$key] ?? [];
    }

    public function hLen(string $key): int
    {
        return count($this->hashes[$key] ?? []);
    }

    public function sAdd(string $key, string $member): void
    {
        $set = $this->sets[$key] ?? [];
        if (!in_array($member, $set, true)) {
            $set[] = $member;
        }
        $this->sets[$key] = $set;
    }

    public function sRem(string $key, string $member): void
    {
        $this->sets[$key] = array_values(array_filter(
            $this->sets[$key] ?? [],
            static fn(string $m): bool => $m !== $member,
        ));
    }

    public function sMembers(string $key): array
    {
        return $this->sets[$key] ?? [];
    }

    public function del(string $key): void
    {
        unset($this->hashes[$key], $this->sets[$key], $this->strings[$key]);
    }

    public function publish(string $channel, string $message): void
    {
        $this->published[] = ['channel' => $channel, 'message' => $message];
    }

    public function setEx(string $key, string $value, int $ttlSeconds): void
    {
        // TTL is irrelevant to the in-memory fake; tests simulate expiry by unsetting.
        $this->strings[$key] = $value;
    }

    public function setNx(string $key, string $value, int $ttlSeconds): bool
    {
        if (array_key_exists($key, $this->strings)) {
            return false;
        }
        $this->strings[$key] = $value;

        return true;
    }

    public function exists(string $key): bool
    {
        return array_key_exists($key, $this->strings)
            || array_key_exists($key, $this->hashes)
            || array_key_exists($key, $this->sets);
    }

    public function get(string $key): ?string
    {
        return $this->strings[$key] ?? null;
    }
}
