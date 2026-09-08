<?php

declare(strict_types=1);

/**
 * Lets the relay go when the worker does — the other half of {@see RelayListener}.
 *
 * NOT housekeeping: without it the worker cannot shut down at all. A
 * subscribed connection is parked on a blocking read that no flag can
 * interrupt and coroutine cancellation does not reliably reach; the only
 * thing that wakes it is its socket closing, which is what stop() does. A
 * coroutine that never finishes is a drain that never completes — every stop
 * and reload would wait out the full window and then be force-killed, losing
 * exactly the in-flight work the drain window exists to protect.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Bridge;

use PHPdot\Realtime\Adapter\RedisSubscriber;
use PHPdot\Server\Attribute\ServerListener;
use PHPdot\Server\Event\WorkerExiting;
use Psr\Container\ContainerInterface;

#[ServerListener]
final class RelayHalt
{
    public function __construct(private readonly ContainerInterface $container) {}

    /**
     * @param WorkerExiting $event The lifecycle event
     *
     * @return void
     */
    public function __invoke(WorkerExiting $event): void
    {
        if (!$this->container->has(RedisSubscriber::class)) {
            return;
        }

        $subscriber = $this->container->get(RedisSubscriber::class);

        if ($subscriber instanceof RedisSubscriber) {
            $subscriber->stop();
        }
    }
}
