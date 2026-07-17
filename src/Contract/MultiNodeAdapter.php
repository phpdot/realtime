<?php

declare(strict_types=1);

/**
 * An {@see Adapter} whose membership spans multiple server instances, so its cross-node
 * state must be kept honest by periodic liveness maintenance: each node republishes its
 * own liveness (heartbeat) and reclaims the membership of peers whose liveness has expired
 * (reap). Single-node adapters (e.g. TableAdapter) do NOT implement this — there is no peer
 * state to maintain.
 *
 * The maintenance policy (intervals/TTLs, orchestration) lives in
 * {@see \PHPdot\Realtime\Maintenance\ClusterMaintenance}; the transport supplies only the
 * scheduler. A consumer decides whether maintenance is needed with
 * `$adapter instanceof MultiNodeAdapter` — never a concrete class check.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Contract;

use Closure;

interface MultiNodeAdapter extends Adapter
{
    /**
     * Republish this node's liveness key with the given TTL (seconds), so peers know it is
     * alive. Call on a cadence a few times shorter than the TTL, so a couple of misses do
     * not declare the node dead.
     *
     * @param int $ttlSeconds
     *
     * @return void
     */
    public function heartbeatNode(int $ttlSeconds): void;

    /**
     * Reclaim the membership of every peer node whose liveness has expired — the only way a
     * crashed node's leaked rooms/presence get cleaned (it ran no teardown). Coordinated
     * cluster-wide by a per-dead-node lock; never reaps self. Returns the number of
     * connections reaped.
     *
     * @param int $lockTtlSeconds TTL of the per-dead-node reap lock.
     * @param (Closure(): void)|null $yield Called between batches to pace Redis under a large dead node.
     *
     * @return int
     */
    public function reap(int $lockTtlSeconds = 30, ?Closure $yield = null): int;

    /**
     * Publish this node's stats snapshot to shared Redis, under a node-scoped key, with the
     * given TTL — so a dead node's stats expire on their own (like its liveness) and a single
     * reader (a dashboard/exporter) can see the whole cluster from one place. The snapshot is
     * an arbitrary scalar map (counts and gauges the app assembles); the key scheme is owned
     * here, mirroring {@see heartbeatNode()}.
     *
     * @param array<string, int|float|string> $stats
     * @param int $ttlSeconds
     *
     * @return void
     */
    public function publishStats(array $stats, int $ttlSeconds): void;

    /**
     * Read the whole cluster's stats in one shot: every registered node, whether it is
     * currently reporting (its snapshot exists, i.e. was published within its TTL), that
     * snapshot, and cluster-wide totals across reporting nodes. The single read a dashboard
     * or metrics exporter needs — no round-trip to the nodes themselves.
     *
     * @return array{
     *     totals: array{
     *         nodes: int, reporting: int, connections: int,
     *         messagesIn: int, pushesOk: int, pushesFailed: int,
     *     },
     *     nodes: list<array{nodeId: string, reporting: bool, stats: array<string, int|float|string>|null}>
     * }
     */
    public function clusterStats(): array;
}
