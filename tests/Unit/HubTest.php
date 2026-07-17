<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Unit;

use PHPdot\Realtime\Adapter\TableAdapter;
use PHPdot\Realtime\Hub;
use PHPdot\Realtime\Socket;
use PHPdot\Realtime\Tests\Support\FakeSender;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\ServerRequestInterface;

/**
 * Locks the Hub engine (rooms, presence, broadcast targeting, disconnect) against
 * a real TableAdapter and an in-memory sender — no server present. This is the
 * standalone safety net for the SR-M1 extraction/rewire.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class HubTest extends TestCase
{
    private FakeSender $sender;

    private Hub $hub;

    protected function setUp(): void
    {
        $this->sender = new FakeSender();
        $this->hub = new Hub(new TableAdapter($this->sender), $this->sender);
    }

    #[Test]
    public function openRegistersSocketAndFiresConnectionHandlers(): void
    {
        $captured = null;
        $this->hub->onConnection(function (Socket $socket) use (&$captured): void {
            $captured = $socket;
        });

        $socket = $this->open(1);

        self::assertSame($socket, $captured, 'onConnection should receive the new socket');
        self::assertSame(1, $socket->id());
        self::assertSame($socket, $this->hub->socket(1));
    }

    #[Test]
    public function joinSendsPresenceHereToJoinerAndPresenceJoinedToOthers(): void
    {
        $s1 = $this->open(1);
        $s1->setUser(['id' => 'u1', 'name' => 'A']);
        $s1->join('room');

        $s2 = $this->open(2);
        $s2->setUser(['id' => 'u2', 'name' => 'B']);
        $s2->join('room');

        // First joiner: only its own (empty) roster.
        self::assertSame(['presence:here', 'presence:joined'], $this->sender->eventNamesTo(1));
        self::assertSame(['presence:here'], $this->sender->eventNamesTo(2));

        // s2's presence:here roster contains the already-present u1.
        $s2Here = $this->sender->eventsTo(2)[0]['data'];
        self::assertIsArray($s2Here);
        self::assertSame('u1', $s2Here['members'][0]['id']);

        // s1 is told u2 joined.
        $s1Joined = $this->sender->eventsTo(1)[1]['data'];
        self::assertIsArray($s1Joined);
        self::assertSame('u2', $s1Joined['user']['id']);
        self::assertSame(2, $s1Joined['fd']);
    }

    #[Test]
    public function toIsLiteralAndIncludesTheSender(): void
    {
        $this->joinRoom(1, 'u1');
        $this->joinRoom(2, 'u2');
        $this->joinRoom(3, 'u3');

        $this->hub->socket(1)?->to('room')->emit('msg', ['t' => 'hi']);

        foreach ([1, 2, 3] as $fd) {
            self::assertContains('msg', $this->sender->eventNamesTo($fd), "fd {$fd} should receive to('room')");
        }
    }

    #[Test]
    public function socketBroadcastExcludesSelf(): void
    {
        $this->joinRoom(1, 'u1');
        $this->joinRoom(2, 'u2');

        $this->hub->socket(1)?->broadcast()->to('room')->emit('m');

        self::assertNotContains('m', $this->sender->eventNamesTo(1), 'broadcast() must exclude the sender');
        self::assertContains('m', $this->sender->eventNamesTo(2));
    }

    #[Test]
    public function hubEmitReachesEveryConnection(): void
    {
        $this->open(1);
        $this->open(2);
        $this->open(3);

        $this->hub->emit('global', ['v' => 1]);

        foreach ([1, 2, 3] as $fd) {
            self::assertContains('global', $this->sender->eventNamesTo($fd));
        }
    }

    #[Test]
    public function leaveSendsPresenceLeftAndRemovesFromRoom(): void
    {
        $this->joinRoom(1, 'u1');
        $this->joinRoom(2, 'u2');
        self::assertSame(2, $this->hub->room('room')->count());

        $this->hub->socket(2)?->leave('room');

        self::assertContains('presence:left', $this->sender->eventNamesTo(1));
        self::assertSame(1, $this->hub->room('room')->count());
    }

    #[Test]
    public function closeFiresDisconnectHandlerBroadcastsPresenceLeftAndCleansUp(): void
    {
        $this->joinRoom(1, 'u1');
        $s2 = $this->joinRoom(2, 'u2');

        $fired = false;
        $s2->onDisconnect(function () use (&$fired): void {
            $fired = true;
        });

        $this->hub->handleClose(2);

        self::assertTrue($fired, 'onDisconnect should fire on close');
        self::assertNull($this->hub->socket(2), 'the socket should be gone after close');
        self::assertContains('presence:left', $this->sender->eventNamesTo(1));
        self::assertSame(1, $this->hub->room('room')->count());
    }

    #[Test]
    public function subscribeJoinsRoomSilentlyWithoutPresence(): void
    {
        $s1 = $this->open(1);
        $s1->subscribe('feed');

        // A silent pub/sub subscription emits no presence frames at all.
        self::assertSame([], $this->sender->eventNamesTo(1));

        // But the fd IS in the room, so broadcasts still reach it.
        $this->hub->to('feed')->emit('tick', ['v' => 1]);
        self::assertSame(['tick'], $this->sender->eventNamesTo(1));
    }

    #[Test]
    public function closingASilentSubscriberEmitsNoPresenceLeft(): void
    {
        $this->open(1)->subscribe('feed');
        $this->open(2)->subscribe('feed');

        $this->hub->handleClose(1);

        // The other silent subscriber must NOT be told anyone left.
        self::assertNotContains('presence:left', $this->sender->eventNamesTo(2));
        self::assertNull($this->hub->socket(1));
        self::assertSame(1, $this->hub->room('feed')->count());
    }

    #[Test]
    public function unsubscribeLeavesRoomSilently(): void
    {
        $this->open(1)->subscribe('feed');
        $this->open(2)->subscribe('feed');

        $this->hub->socket(1)?->unsubscribe('feed');

        self::assertNotContains('presence:left', $this->sender->eventNamesTo(2));
        self::assertSame(1, $this->hub->room('feed')->count());
    }

    #[Test]
    public function heartbeatPingsLiveConnectionsAndReapsIdleOnes(): void
    {
        $this->open(1);
        $this->open(2);
        $this->hub->touch(1, 1000.0); // fresh
        $this->hub->touch(2, 900.0);  // stale

        // now=1010, idleTimeout=30: fd1 idle=10 (keep+ping), fd2 idle=110 (reap).
        $reaped = $this->hub->heartbeat(30.0, 1010.0);

        self::assertSame(1, $reaped);
        self::assertSame([1], $this->sender->pinged, 'only the live fd is pinged');
        self::assertSame(
            [['fd' => 2, 'code' => 1001, 'reason' => 'heartbeat timeout']],
            $this->sender->disconnected,
            'the idle fd is disconnected',
        );
    }

    #[Test]
    public function disconnectUserClosesEveryFdOfThatUser(): void
    {
        $this->open(1)->setUser(['id' => 'u1']);
        $this->open(2)->setUser(['id' => 'u1']); // same user, second device
        $this->open(3)->setUser(['id' => 'other']);

        $this->hub->disconnectUser('u1');

        $closedFds = array_map(static fn(array $d): int => $d['fd'], $this->sender->disconnected);
        sort($closedFds);
        self::assertSame([1, 2], $closedFds);
    }

    #[Test]
    public function roomFacadeReportsMembersAndCount(): void
    {
        $this->joinRoom(1, 'u1');
        $this->joinRoom(2, 'u2');

        $room = $this->hub->room('room');
        self::assertSame(2, $room->count());

        $ids = array_map(static fn(array $m): mixed => $m['user']['id'] ?? null, $room->members());
        sort($ids);
        self::assertSame(['u1', 'u2'], $ids);
    }

    private function open(int $fd): Socket
    {
        $this->hub->handleOpen($fd, $this->createStub(ServerRequestInterface::class));
        $socket = $this->hub->socket($fd);
        self::assertNotNull($socket);

        return $socket;
    }

    private function joinRoom(int $fd, string $userId): Socket
    {
        $socket = $this->open($fd);
        $socket->setUser(['id' => $userId]);
        $socket->join('room');

        return $socket;
    }
}
