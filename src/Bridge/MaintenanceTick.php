<?php

declare(strict_types=1);

/**
 * Runs the cluster's membership maintenance — node heartbeat and peer
 * reaping — on the timers {@see ClusterMaintenance} states its intervals for.
 *
 * Without it, membership from an ungraceful exit accumulates forever: the
 * maintenance class ships the logic and nothing in the package ever calls
 * it. Worker 0 only — one node entry, one reaper, not one per worker.
 *
 * Each beat runs in its OWN coroutine with a watchdog, and {@see
 * MaintenanceHalt} clears the timers AND cancels the in-flight beat: socket
 * work parked on an unanswering peer outlives every configured timeout
 * under the coroutine hooks, and a parked beat holds the worker drain for
 * its full window no matter who clears which timer.
 *
 * Discovers nothing on its own: the host binds `ClusterMaintenance::class`
 * (over a MultiNodeAdapter), and this listener runs what was bound. A
 * single-node TableAdapter never reaches a timer.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Bridge;

use Closure;
use PHPdot\Container\Attribute\Singleton;
use PHPdot\Realtime\Maintenance\ClusterMaintenance;
use PHPdot\Server\Attribute\ServerListener;
use PHPdot\Server\Event\WorkerStarted;

#[ServerListener]
#[Singleton]
final class MaintenanceTick
{
    /**
     * Wall-clock bound on one beat — the watchdog cancels what a parked
     * socket read would otherwise hold forever.
     */
    private const int BEAT_LIMIT_MS = 5000;

    /** @var array<int, true> The armed ticks' ids */
    private array $tickIds = [];

    /** @var array<int, true> The in-flight beats' coroutines */
    private array $beatCids = [];

    public function __construct(private readonly ClusterMaintenance $maintenance) {}

    /**
     * @param WorkerStarted $event The lifecycle event
     *
     * @return void
     */
    public function __invoke(WorkerStarted $event): void
    {
        if ($event->workerId !== 0 || !$this->maintenance->required()) {
            return;
        }

        $this->arm($this->maintenance->heartbeatIntervalMs(), $this->maintenance->heartbeat(...));
        $this->arm($this->maintenance->reapIntervalMs(), $this->maintenance->reap(...));
    }

    /**
     * Cancel the timers AND the in-flight beats — the worker-exit call.
     *
     * @return void
     */
    public function halt(): void
    {
        foreach (array_keys($this->tickIds) as $id) {
            \Swoole\Timer::clear($id);
        }

        $this->tickIds = [];

        foreach (array_keys($this->beatCids) as $cid) {
            if (\Swoole\Coroutine::exists($cid)) {
                \Swoole\Coroutine::cancel($cid);
            }
        }

        $this->beatCids = [];
    }

    /**
     * One timer whose every fire is a bounded, cancellable coroutine.
     *
     * @param int $intervalMs Cadence
     * @param Closure $beat The maintenance call
     *
     * @return void
     */
    private function arm(int $intervalMs, Closure $beat): void
    {
        $tickId = \Swoole\Timer::tick($intervalMs, function () use ($beat): void {
            $cid = \Swoole\Coroutine::create(static function () use ($beat): void {
                $beat();
            });

            if ($cid === false) {
                return;
            }

            $this->beatCids[$cid] = true;

            \Swoole\Timer::after(self::BEAT_LIMIT_MS, function () use ($cid): void {
                unset($this->beatCids[$cid]);

                if (\Swoole\Coroutine::exists($cid)) {
                    \Swoole\Coroutine::cancel($cid);
                }
            });
        });

        if ($tickId !== false) {
            $this->tickIds[$tickId] = true;
        }
    }
}
