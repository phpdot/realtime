<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Unit;

use PHPdot\Realtime\Adapter\RedisAdapter;
use PHPdot\Realtime\Tests\Support\FakeRedis;
use PHPdot\Realtime\Tests\Support\FakeSender;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

/**
 * Locks dead-node reaping: a node that crashes leaves membership behind (no delAll ever
 * runs), and a live peer must reclaim it once its liveness heartbeat expires — without
 * touching live nodes, itself, or a node another peer is already reaping.
 *
 * Two adapters share one FakeRedis to model two nodes; a crash is simulated by dropping
 * the dead node's liveness key (what TTL expiry does in real Redis).
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class RedisReaperTest extends TestCase
{
    private FakeRedis $redis;

    private RedisAdapter $live;

    protected function setUp(): void
    {
        $this->redis = new FakeRedis();
        $sender = new FakeSender();
        $this->live = new RedisAdapter(fn(): FakeRedis => $this->redis, $sender, 'live-node');
        $this->live->heartbeatNode(30);
    }

    /** Register a second node with membership, then simulate its ungraceful death. */
    private function crashNodeWith(string $nodeId, int $fd, array $rooms, ?array $user): void
    {
        $sender = new FakeSender();
        $dead = new RedisAdapter(fn(): FakeRedis => $this->redis, $sender, $nodeId);
        $dead->heartbeatNode(30);
        $dead->add($fd, $rooms, $user);
        // Ungraceful exit: no delAll; liveness key expires.
        unset($this->redis->strings['rt:node:' . $nodeId]);
    }

    #[Test]
    public function addAndDelAllMaintainThePerNodeConnIndex(): void
    {
        $this->live->add(7, ['room']);
        self::assertSame(['7'], $this->redis->sets['rt:nodeconns:live-node'] ?? []);

        $this->live->delAll(7);
        self::assertSame([], $this->redis->sets['rt:nodeconns:live-node'] ?? []);
    }

    #[Test]
    public function heartbeatRegistersLivenessAndNodeSet(): void
    {
        self::assertSame('1', $this->redis->strings['rt:node:live-node'] ?? null);
        self::assertContains('live-node', $this->redis->sets['rt:nodes'] ?? []);
    }

    #[Test]
    public function reapsADeadNodesLeakedMembership(): void
    {
        $this->crashNodeWith('dead-node', 5, ['room'], ['id' => 'u1', 'name' => 'A']);

        $reaped = $this->live->reap();

        self::assertSame(1, $reaped);
        self::assertArrayNotHasKey('rt:conn:dead-node:5', $this->redis->hashes, 'conn key gone');
        self::assertArrayNotHasKey('dead-node:5', $this->redis->hashes['rt:room:room'] ?? [], 'room field gone');
        self::assertNotContains('dead-node:5', $this->redis->sets['rt:user:u1'] ?? [], 'user member gone');
        self::assertArrayNotHasKey('rt:nodeconns:dead-node', $this->redis->sets, 'conn index gone');
        self::assertNotContains('dead-node', $this->redis->sets['rt:nodes'] ?? [], 'deregistered');
    }

    #[Test]
    public function reapCleansEveryRoomAndUserOfADeadMultiRoomConnection(): void
    {
        $this->crashNodeWith('dead-node', 9, ['a', 'b', 'c'], ['id' => 'u9']);

        $this->live->reap();

        self::assertArrayNotHasKey('dead-node:9', $this->redis->hashes['rt:room:a'] ?? []);
        self::assertArrayNotHasKey('dead-node:9', $this->redis->hashes['rt:room:b'] ?? []);
        self::assertArrayNotHasKey('dead-node:9', $this->redis->hashes['rt:room:c'] ?? []);
        self::assertNotContains('dead-node:9', $this->redis->sets['rt:user:u9'] ?? []);
    }

    #[Test]
    public function leavesLiveNodesUntouched(): void
    {
        $sender = new FakeSender();
        $other = new RedisAdapter(fn(): FakeRedis => $this->redis, $sender, 'other-node');
        $other->heartbeatNode(30); // liveness present → alive
        $other->add(3, ['room'], ['id' => 'u3']);

        $reaped = $this->live->reap();

        self::assertSame(0, $reaped);
        self::assertArrayHasKey('rt:conn:other-node:3', $this->redis->hashes, 'live node membership kept');
        self::assertContains('other-node', $this->redis->sets['rt:nodes'] ?? []);
    }

    #[Test]
    public function neverReapsItself(): void
    {
        $this->live->add(1, ['room'], ['id' => 'me']);
        // Even if our OWN liveness lapsed, self is skipped (we are running, by definition).
        unset($this->redis->strings['rt:node:live-node']);

        $reaped = $this->live->reap();

        self::assertSame(0, $reaped);
        self::assertArrayHasKey('rt:conn:live-node:1', $this->redis->hashes);
    }

    #[Test]
    public function skipsADeadNodeAnotherPeerIsAlreadyReaping(): void
    {
        $this->crashNodeWith('dead-node', 5, ['room'], ['id' => 'u1']);
        // A peer holds the reap lock.
        $this->redis->strings['rt:reaping:dead-node'] = 'peer-node';

        $reaped = $this->live->reap();

        self::assertSame(0, $reaped);
        self::assertArrayHasKey('rt:conn:dead-node:5', $this->redis->hashes, 'left for the lock holder');
    }
}
