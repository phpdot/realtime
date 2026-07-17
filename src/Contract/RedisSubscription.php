<?php

declare(strict_types=1);

/**
 * RedisSubscription — the blocking SUBSCRIBE side of the multi-node relay, kept
 * separate from {@see RedisCommands} because pub/sub monopolises a socket: a connection
 * in SUBSCRIBE state can issue no other commands, so it must never be shared with the
 * command channel or borrowed from the command pool.
 *
 * The consuming app (dot wraps phpdot/redis) supplies an implementation over a
 * DEDICATED connection. Under Swoole's coroutine runtime hooks the blocking
 * ext-redis `subscribe()` yields the coroutine on the socket rather than freezing the
 * worker, so one worker can both serve connections and run this loop.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Contract;

use Closure;

interface RedisSubscription
{
    /**
     * Block on a SUBSCRIBE to $channel, invoking $onMessage($payload) for every
     * message received, until the connection drops (the caller reconnects). MUST run
     * inside a coroutine.
     *
     * @param Closure(string): void $onMessage Receives each raw published payload.
     * @param string $channel
     *
     * @return void
     */
    public function subscribe(string $channel, Closure $onMessage): void;

    /**
     * Close the underlying connection, unblocking a subscribe() parked on it (the socket read
     * errors out). This is how the loop is stopped from another coroutine on worker exit —
     * a flag can't wake a blocking SUBSCRIBE, and cancelling the coroutine is unreliable on
     * ext-redis. Safe to call from any coroutine and on an already-closed connection.
     *
     * @return void
     */
    public function close(): void;
}
