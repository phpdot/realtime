<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Unit;

use Closure;
use PHPdot\Realtime\Adapter\RedisAdapter;
use PHPdot\Realtime\Adapter\RedisSubscriber;
use PHPdot\Realtime\Contract\RedisSubscription;
use PHPdot\Realtime\Tests\Support\FakeRedis;
use PHPdot\Realtime\Tests\Support\FakeSender;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * Locks the subscriber's one job beyond a plain loop: SURVIVE a dropped SUBSCRIBE and
 * re-subscribe (a worker that stops re-subscribing goes silently deaf to broadcasts),
 * and route each payload into the adapter's local fan-out.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class RedisSubscriberTest extends TestCase
{
    #[Test]
    public function reconnectsAfterADroppedSubscribeAndDeliversToLocalFds(): void
    {
        $redis = new FakeRedis();
        $sender = new FakeSender();
        $adapter = new RedisAdapter(fn(): FakeRedis => $redis, $sender, 'nodeA');
        $adapter->add(1, ['room']);

        $envelope = json_encode([
            'node' => 'nodeB',
            'rooms' => ['room'],
            'except' => [],
            'frame' => 'HI',
        ], JSON_THROW_ON_ERROR);

        // First subscribe throws (a drop); the second delivers one frame then returns.
        $subscription = new class ($envelope) implements RedisSubscription {
            public int $calls = 0;

            public bool $closed = false;

            public function __construct(private readonly string $envelope) {}

            public function subscribe(string $channel, Closure $onMessage): void
            {
                $this->calls++;
                if ($this->calls === 1) {
                    throw new RuntimeException('connection dropped');
                }
                $onMessage($this->envelope);
                // Returns → simulates the connection dropping again.
            }

            public function close(): void
            {
                $this->closed = true;
            }
        };

        $backoffs = 0;
        $subscriber = new RedisSubscriber(
            $adapter,
            fn(): RedisSubscription => $subscription,
            function () use (&$backoffs, &$subscriber): void {
                $backoffs++;
                if ($backoffs >= 2) {
                    $subscriber->stop();
                }
            },
        );

        $subscriber->run();

        self::assertSame(['HI'], $sender->sent[1] ?? [], 'delivered after reconnecting past the drop');
        self::assertGreaterThanOrEqual(2, $subscription->calls, 'must re-subscribe after a drop');
        self::assertSame(2, $backoffs);
    }

    #[Test]
    public function stopPreventsFurtherResubscribes(): void
    {
        $redis = new FakeRedis();
        $adapter = new RedisAdapter(fn(): FakeRedis => $redis, new FakeSender(), 'nodeA');

        $subscription = new class implements RedisSubscription {
            public int $calls = 0;

            public bool $closed = false;

            public function subscribe(string $channel, Closure $onMessage): void
            {
                $this->calls++;
                // Returns immediately (a drop) every time.
            }

            public function close(): void
            {
                $this->closed = true;
            }
        };

        $subscriber = new RedisSubscriber(
            $adapter,
            fn(): RedisSubscription => $subscription,
            function () use (&$subscriber): void {
                $subscriber->stop();
            },
        );

        $subscriber->run();

        self::assertSame(1, $subscription->calls, 'stop() after the first drop halts reconnection');
        self::assertTrue($subscription->closed, 'stop() closes the in-flight subscription to unblock a parked SUBSCRIBE');
    }
}
