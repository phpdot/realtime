<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Unit;

use PHPdot\Realtime\Adapter\TableAdapter;
use PHPdot\Realtime\Tests\Support\FakeSender;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

/**
 * Locks the TableAdapter contract directly — especially the rewired broadcast()
 * that now iterates the adapter's OWN connections table through the sender seam
 * (no concrete server).
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class TableAdapterTest extends TestCase
{
    private FakeSender $sender;

    private TableAdapter $adapter;

    protected function setUp(): void
    {
        $this->sender = new FakeSender();
        $this->adapter = new TableAdapter($this->sender);
    }

    #[Test]
    public function tracksRoomsUserIdentityAndUserFds(): void
    {
        $this->adapter->add(1, ['room'], ['id' => 'u1', 'name' => 'A']);

        self::assertSame(['room'], $this->adapter->roomsOf(1));
        self::assertSame(['id' => 'u1', 'name' => 'A'], $this->adapter->userOf(1));
        self::assertSame([1], $this->adapter->fdsOfUser('u1'));
        self::assertSame(1, $this->adapter->count(['room']));
    }

    #[Test]
    public function globalBroadcastReachesEveryConnectionExceptExcluded(): void
    {
        $this->adapter->add(1, []);
        $this->adapter->add(2, []);
        $this->adapter->add(3, []);

        $this->adapter->broadcast('FRAME', [], [2]);

        self::assertSame(['FRAME'], $this->sender->sent[1] ?? []);
        self::assertArrayNotHasKey(2, $this->sender->sent, 'excluded fd must not receive');
        self::assertSame(['FRAME'], $this->sender->sent[3] ?? []);
    }

    #[Test]
    public function roomBroadcastReachesOnlyRoomMembers(): void
    {
        $this->adapter->add(1, ['room']);
        $this->adapter->add(2, ['room']);
        $this->adapter->add(3, ['other']);

        $this->adapter->broadcast('FRAME', ['room'], []);

        self::assertSame(['FRAME'], $this->sender->sent[1] ?? []);
        self::assertSame(['FRAME'], $this->sender->sent[2] ?? []);
        self::assertArrayNotHasKey(3, $this->sender->sent, 'a non-member must not receive');
    }

    #[Test]
    public function sendPushesToOneFd(): void
    {
        $this->adapter->send(9, 'HELLO');

        self::assertSame(['HELLO'], $this->sender->sent[9] ?? []);
    }

    #[Test]
    public function delRemovesFromRoomButKeepsTheConnection(): void
    {
        $this->adapter->add(1, ['a', 'b']);

        $this->adapter->del(1, ['a']);

        self::assertSame(['b'], $this->adapter->roomsOf(1));
        self::assertSame(0, $this->adapter->count(['a']));
        self::assertSame(1, $this->adapter->count(['b']));
    }

    #[Test]
    public function delAllRemovesTheConnectionEntirely(): void
    {
        $this->adapter->add(1, ['room'], ['id' => 'u1']);

        $this->adapter->delAll(1);

        self::assertSame([], $this->adapter->roomsOf(1));
        self::assertNull($this->adapter->userOf(1));
        self::assertSame([], $this->adapter->fdsOfUser('u1'));
        self::assertSame(0, $this->adapter->count(['room']));
    }
}
