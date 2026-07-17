<?php

declare(strict_types=1);

/**
 * ClusterMaintenance — the multi-node liveness policy, kept out of any transport. Wraps the
 * bound {@see Adapter} and drives the two periodic operations that keep cross-node membership
 * honest when that adapter is a {@see MultiNodeAdapter}:
 *
 * - heartbeat(): republish this node's liveness (every heartbeatIntervalMs, TTL livenessTtl).
 * - reap():      reclaim membership of dead peer nodes (every reapIntervalMs).
 *
 * Both are no-ops on a single-node adapter, so a caller never has to type-check the driver.
 * The transport supplies only the scheduler — a timer on one worker (see the app's
 * RealtimeReaper); the cadences and TTLs live here so nothing hardcodes them. The heartbeat
 * interval is deliberately a few times shorter than the liveness TTL so a couple of missed
 * beats don't declare a live node dead.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Maintenance;

use Closure;
use PHPdot\Realtime\Contract\Adapter;
use PHPdot\Realtime\Contract\MultiNodeAdapter;

final class ClusterMaintenance
{
    /**
     * Run per-node heartbeat and dead-node reaping for a multi-node cluster.
     *
     * @param Adapter $adapter Multi-node adapter to maintain.
     * @param int $heartbeatIntervalMs How often this node republishes its liveness snapshot.
     * @param int $livenessTtlSeconds TTL of a node's liveness key before it is considered dead.
     * @param int $reapIntervalMs How often to scan for and reap dead nodes.
     * @param int $reapLockTtlSeconds TTL of the per-dead-node reap lock.
     */
    public function __construct(
        private readonly Adapter $adapter,
        private readonly int $heartbeatIntervalMs = 10_000,
        private readonly int $livenessTtlSeconds = 30,
        private readonly int $reapIntervalMs = 15_000,
        private readonly int $reapLockTtlSeconds = 30,
    ) {}

    /**
     * Whether the bound adapter needs cluster maintenance at all. False for single-node
     * adapters — the caller can skip scheduling any timers.
     *
     * @return bool
     */
    public function required(): bool
    {
        return $this->adapter instanceof MultiNodeAdapter;
    }

    /**
     * Milliseconds between liveness heartbeats.
     *
     * @return int
     */
    public function heartbeatIntervalMs(): int
    {
        return $this->heartbeatIntervalMs;
    }

    /**
     * Milliseconds between reap sweeps.
     *
     * @return int
     */
    public function reapIntervalMs(): int
    {
        return $this->reapIntervalMs;
    }

    /**
     * Republish this node's liveness. No-op on a single-node adapter.
     *
     * @return void
     */
    public function heartbeat(): void
    {
        if ($this->adapter instanceof MultiNodeAdapter) {
            $this->adapter->heartbeatNode($this->livenessTtlSeconds);
        }
    }

    /**
     * Reclaim dead peers' membership. Returns the number of connections reaped (0 on a
     * single-node adapter).
     *
     * @param (Closure(): void)|null $yield Called between batches to pace Redis under a large dead node.
     *
     * @return int
     */
    public function reap(?Closure $yield = null): int
    {
        return $this->adapter instanceof MultiNodeAdapter
            ? $this->adapter->reap($this->reapLockTtlSeconds, $yield)
            : 0;
    }
}
