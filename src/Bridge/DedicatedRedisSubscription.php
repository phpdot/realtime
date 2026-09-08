<?php

declare(strict_types=1);

/**
 * The realtime relay's subscription, over one dedicated phpdot/redis
 * connection.
 *
 * A subscription is a BLOCKING read for the life of the process — it can
 * never borrow from a pool, because it never hands the connection back. The
 * bridge therefore builds its own from the connect closure it is given:
 *
 *     RedisSubscription::class => singleton(
 *         fn (ContainerInterface $c): RedisSubscription => new DedicatedRedisSubscription(
 *             connect: fn (): RedisConnection => new RedisConnection($c->get(RedisConfig::class)),
 *         ),
 *     );
 *
 * close() drops the socket — the only thing that wakes a subscribe parked on
 * it — which is what {@see RelayHalt} calls on worker exit.
 *
 * The bridge is optional by package position — phpdot/redis sits in
 * require-dev + suggest — and nothing outside Bridge/ names it.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Bridge;

use Closure;
use PHPdot\Realtime\Contract\RedisSubscription;
use PHPdot\Redis\Config\RedisConfig;
use PHPdot\Redis\RedisConnection;
use Throwable;

final class DedicatedRedisSubscription implements RedisSubscription
{
    private null|RedisConnection $connection = null;

    /**
     * @param Closure(): RedisConnection $connect Builds the dedicated connection the subscription parks on
     */
    public function __construct(private readonly Closure $connect) {}

    /**
     * @inheritDoc
     */
    public function subscribe(string $channel, Closure $onMessage): void
    {
        $connection = $this->unbounded(($this->connect)());
        $this->connection = $connection;

        if (!$connection->isConnected()) {
            $connection->connect();
        }

        $connection->getClient()->subscribe(
            [$channel],
            static function (mixed $subscription, mixed $topic, mixed $payload) use ($onMessage): void {
                if (is_string($payload)) {
                    $onMessage($payload);
                }
            },
        );
    }

    /**
     * The subscription's connection, with its read timeout REMOVED.
     *
     * A subscribe is a blocking read for the life of the process — a bounded
     * readTimeout (a host sets one for its pooled connections, correctly)
     * kills the subscription the moment it expires: `read error on
     * connection`, measured with 1s and 3s bounds alike. The thing that ends
     * this read is close() dropping the socket, never a timeout.
     *
     * @param RedisConnection $connection What the connect closure built
     *
     * @return RedisConnection The same connection, read-unbounded
     */
    private function unbounded(RedisConnection $connection): RedisConnection
    {
        $config = new RedisConfig(
            host: $connection->getConfig()->host,
            port: $connection->getConfig()->port,
            path: $connection->getConfig()->path,
            password: $connection->getConfig()->password,
            username: $connection->getConfig()->username,
            database: $connection->getConfig()->database,
            timeout: $connection->getConfig()->timeout,
            retryInterval: $connection->getConfig()->retryInterval,
            readTimeout: 0.0,
            tls: $connection->getConfig()->tls,
            ssl: $connection->getConfig()->ssl,
            maxRetries: $connection->getConfig()->maxRetries,
            persistent: $connection->getConfig()->persistent,
            context: $connection->getConfig()->context,
        );

        return new RedisConnection($config);
    }

    /**
     * @inheritDoc
     */
    public function close(): void
    {
        try {
            $this->connection?->getClient()->close();
        } catch (Throwable) {
            /* Already gone is the outcome asked for. */
        }

        $this->connection = null;
    }
}
