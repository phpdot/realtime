<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Unit;

use PHPdot\Realtime\Adapter\RedisAdapter;
use PHPdot\Realtime\Adapter\TableAdapter;
use PHPdot\Realtime\Maintenance\ClusterMaintenance;
use PHPdot\Realtime\Tests\Support\FakeRedis;
use PHPdot\Realtime\Tests\Support\FakeSender;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

/**
 * Locks the maintenance policy: it drives heartbeat/reap on a MultiNodeAdapter and is a
 * total no-op on a single-node adapter, so the transport driver never type-checks a concrete
 * class. Cadences/TTLs are owned here (defaulted, overridable) — not hardcoded in any app.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class ClusterMaintenanceTest extends TestCase
{
    private FakeRedis $redis;

    private FakeSender $sender;

    protected function setUp(): void
    {
        $this->redis = new FakeRedis();
        $this->sender = new FakeSender();
    }

    private function redisAdapter(): RedisAdapter
    {
        return new RedisAdapter(fn(): FakeRedis => $this->redis, $this->sender, 'nodeA');
    }

    #[Test]
    public function requiredIsTrueForAMultiNodeAdapter(): void
    {
        self::assertTrue((new ClusterMaintenance($this->redisAdapter()))->required());
    }

    #[Test]
    public function requiredIsFalseForASingleNodeAdapter(): void
    {
        self::assertFalse((new ClusterMaintenance(new TableAdapter($this->sender)))->required());
    }

    #[Test]
    public function heartbeatPublishesThisNodesLiveness(): void
    {
        (new ClusterMaintenance($this->redisAdapter()))->heartbeat();

        // heartbeatNode() sets the liveness key and registers the node in the cluster set.
        self::assertSame('1', $this->redis->strings['rt:node:nodeA'] ?? null);
        self::assertContains('nodeA', $this->redis->sets['rt:nodes'] ?? []);
    }

    #[Test]
    public function reapReturnsZeroWhenNoPeerIsDead(): void
    {
        $maintenance = new ClusterMaintenance($this->redisAdapter());
        $maintenance->heartbeat(); // only this node registered, and it is alive

        self::assertSame(0, $maintenance->reap());
    }

    #[Test]
    public function heartbeatAndReapAreNoOpsForASingleNodeAdapter(): void
    {
        $maintenance = new ClusterMaintenance(new TableAdapter($this->sender));

        $maintenance->heartbeat(); // must not throw
        self::assertSame(0, $maintenance->reap());
        self::assertSame([], $this->redis->strings); // nothing touched Redis
    }

    #[Test]
    public function intervalsExposeSensibleDefaults(): void
    {
        $maintenance = new ClusterMaintenance($this->redisAdapter());

        self::assertSame(10_000, $maintenance->heartbeatIntervalMs());
        self::assertSame(15_000, $maintenance->reapIntervalMs());
    }

    #[Test]
    public function intervalsAreOverridable(): void
    {
        $maintenance = new ClusterMaintenance(
            $this->redisAdapter(),
            heartbeatIntervalMs: 5_000,
            reapIntervalMs: 8_000,
        );

        self::assertSame(5_000, $maintenance->heartbeatIntervalMs());
        self::assertSame(8_000, $maintenance->reapIntervalMs());
    }
}
