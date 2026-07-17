<?php

declare(strict_types=1);

/**
 * RedisSubscriber — the receive half of the multi-node relay. One runs per worker: it
 * blocks on a SUBSCRIBE to the adapter's broadcast channel and hands every payload to
 * {@see RedisAdapter::deliver()}, which fans it out to that worker's local fds.
 *
 * A dropped SUBSCRIBE would make the worker go silently deaf to broadcasts, so {@see
 * run()} reconnects: when a subscribe returns or throws it backs off and re-subscribes
 * on a FRESH connection, until {@see stop()}. The subscription factory yields a
 * dedicated connection each time (pub/sub monopolises its socket — never the command
 * pool). Both the SUBSCRIBE and the backoff must yield under Swoole's coroutine runtime
 * so the worker keeps serving connections.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Adapter;

use Closure;
use PHPdot\Realtime\Contract\RedisSubscription;
use Throwable;

final class RedisSubscriber
{
    private bool $stopped = false;

    /**
     * The in-flight subscription, so stop() can close its socket to unblock a parked subscribe.
     */
    private ?RedisSubscription $current = null;

    /**
     * Run the blocking Redis SUBSCRIBE loop on a dedicated connection, feeding messages to the adapter.
     *
     * @param RedisAdapter $adapter Applies decoded broadcast messages locally.
     * @param Closure(): RedisSubscription $subscriptions Yields a fresh dedicated connection per subscribe.
     * @param Closure(): void $backoff Coroutine-yielding pause between reconnect attempts (e.g. Co::sleep).
     */
    public function __construct(
        private readonly RedisAdapter $adapter,
        private readonly Closure $subscriptions,
        private readonly Closure $backoff,
    ) {}

    /**
     * Block on the broadcast channel, re-subscribing across drops, until {@see stop()}.
     * Runs for the life of the worker; launch it in its own coroutine.
     *
     * @return void
     */
    public function run(): void
    {
        $channel = $this->adapter->broadcastChannel();
        $deliver = $this->adapter->deliver(...);

        while (!$this->stopped) {
            try {
                $subscription = ($this->subscriptions)();
                $this->current = $subscription;

                if ($this->stopped) {
                    $subscription->close();

                    break;
                }

                $subscription->subscribe($channel, $deliver);
            } catch (Throwable) {
            }

            if ($this->stopped) {
                break;
            }

            ($this->backoff)();
        }

        $this->current = null;
    }

    /**
     * Stop the loop and unblock it: sets the no-more-reconnect flag AND closes the in-flight
     * subscription's socket, because a flag alone can never wake a subscribe() parked inside
     * ext-redis. Closing the socket makes that read error out, the subscribe throws, and the
     * loop sees stopped and breaks. (Cancelling the coroutine instead is unreliable on
     * ext-redis — it crashes the worker often enough to matter.) Safe to call from another
     * coroutine, e.g. the transport's onWorkerExit.
     *
     * @return void
     */
    public function stop(): void
    {
        $this->stopped = true;
        $this->current?->close();
    }
}
