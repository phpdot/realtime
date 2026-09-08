<?php

declare(strict_types=1);

/**
 * Starts the cluster relay's receive half — one subscription per worker.
 *
 * A broadcast is PUBLISHed once and has to reach the connections every OTHER
 * worker owns, including other workers on this same machine: a reader's two
 * tabs land wherever the balancer put them. Without this listener a room is
 * silently only ever the sender's own worker, which looks exactly like a
 * working chat until a second tab is opened.
 *
 * Per worker, not once: only the worker holding an fd can push to it. The
 * subscription runs in its own coroutine — subscribe() never returns while
 * the connection lives — and {@see RelayHalt} stops it on worker exit.
 *
 * Discovers nothing on its own: the host binds `RedisSubscriber::class` (over
 * {@see DedicatedRedisSubscription}) and this listener starts what was bound.
 * A host without the binding — or without phpdot/redis installed — runs the
 * single-node TableAdapter and never reaches this coroutine.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Bridge;

use PHPdot\Realtime\Adapter\RedisSubscriber;
use PHPdot\Server\Attribute\ServerListener;
use PHPdot\Server\Event\WorkerStarted;
use Psr\Container\ContainerInterface;
use Swoole\Coroutine;
use Throwable;

#[ServerListener]
final class RelayListener
{
    public function __construct(private readonly ContainerInterface $container) {}

    /**
     * @param WorkerStarted $event The lifecycle event
     *
     * @return void
     */
    public function __invoke(WorkerStarted $event): void
    {
        if (!class_exists(\PHPdot\Redis\RedisConnection::class) || !$this->container->has(RedisSubscriber::class)) {
            return;
        }

        $subscriber = $this->container->get(RedisSubscriber::class);

        if (!$subscriber instanceof RedisSubscriber) {
            return;
        }

        Coroutine::create(static function () use ($subscriber): void {
            try {
                $subscriber->run();
            } catch (Throwable $error) {
                error_log('[realtime] relay stopped: ' . $error->getMessage());
            }
        });
    }
}
