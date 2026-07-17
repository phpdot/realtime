<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Unit;

use PHPdot\Realtime\Adapter\RedisAdapter;
use PHPdot\Realtime\Tests\Support\FakeRedis;
use PHPdot\Realtime\Tests\Support\FakeSender;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

/**
 * Locks the RedisAdapter contract: membership/identity live in Redis (global,
 * cross-node), broadcasts RELAY over pub/sub (publish-only — no direct push), and the
 * per-fd operations that must stay local (fdsOfUser) only surface THIS node's fds.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class RedisAdapterTest extends TestCase
{
    private FakeRedis $redis;

    private FakeSender $sender;

    private RedisAdapter $adapter;

    protected function setUp(): void
    {
        $this->redis = new FakeRedis();
        $this->sender = new FakeSender();
        $this->adapter = new RedisAdapter(fn(): FakeRedis => $this->redis, $this->sender, 'nodeA');
    }

    #[Test]
    public function addStoresRoomsIdentityAndUserFdsInRedis(): void
    {
        $this->adapter->add(1, ['room'], ['id' => 'u1', 'name' => 'A']);

        self::assertSame(['room'], $this->adapter->roomsOf(1));
        self::assertSame(['id' => 'u1', 'name' => 'A'], $this->adapter->userOf(1));
        self::assertSame([1], $this->adapter->fdsOfUser('u1'));
        self::assertSame(1, $this->adapter->count(['room']));
    }

    #[Test]
    public function addMergesRoomsAcrossCalls(): void
    {
        $this->adapter->add(1, ['a']);
        $this->adapter->add(1, ['b']);

        self::assertSame(['a', 'b'], $this->adapter->roomsOf(1));
    }

    #[Test]
    public function broadcastPublishesRelayEnvelopeWithoutPushing(): void
    {
        $this->adapter->broadcast('FRAME', ['room'], [3]);

        self::assertSame([], $this->sender->sent, 'broadcast must NOT push directly — it relays');
        self::assertCount(1, $this->redis->published);

        $published = $this->redis->published[0];
        self::assertSame('rt:broadcast', $published['channel']);

        $envelope = json_decode($published['message'], true);
        self::assertIsArray($envelope);
        self::assertSame('nodeA', $envelope['node']);
        self::assertSame(['room'], $envelope['rooms']);
        self::assertSame([3], $envelope['except']);
        self::assertSame('FRAME', $envelope['frame']);
    }

    #[Test]
    public function sendPushesToOneLocalFd(): void
    {
        $this->adapter->send(9, 'HELLO');

        self::assertSame(['HELLO'], $this->sender->sent[9] ?? []);
    }

    #[Test]
    public function membersReturnsTheGlobalRosterAcrossNodes(): void
    {
        // A local member plus a member owned by a DIFFERENT node, seeded straight into
        // the shared room hash the way a remote node's add() would.
        $this->adapter->add(1, ['room'], ['id' => 'u1', 'name' => 'A']);
        $this->redis->hSet('rt:room:room', 'nodeB:7', json_encode(['id' => 'u2', 'name' => 'B'], JSON_THROW_ON_ERROR));

        $members = $this->adapter->members(['room']);

        self::assertCount(2, $members);
        $byFd = [];
        foreach ($members as $m) {
            $byFd[$m['fd']] = $m['user'];
        }
        self::assertSame(['id' => 'u1', 'name' => 'A'], $byFd[1] ?? null);
        self::assertSame(['id' => 'u2', 'name' => 'B'], $byFd[7] ?? null, 'roster must include the remote node member');
        self::assertSame(2, $this->adapter->count(['room']));
    }

    #[Test]
    public function fdsOfUserReturnsOnlyThisNodesFds(): void
    {
        // Same user connected on this node (fd 5) and on another node (fd 7). A node can
        // only push to its own fds, so fdsOfUser must hide the remote one.
        $this->adapter->add(5, [], ['id' => 'u1']);
        $this->redis->sAdd('rt:user:u1', 'nodeB:7');

        self::assertSame([5], $this->adapter->fdsOfUser('u1'));
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

    #[Test]
    public function countDeduplicatesAcrossMultipleRooms(): void
    {
        $this->adapter->add(1, ['a', 'b']);
        $this->adapter->add(2, ['b']);

        self::assertSame(2, $this->adapter->count(['a', 'b']));
    }

    // --- deliver(): the relayed fan-out to LOCAL fds ---

    #[Test]
    public function deliverFansRoomFrameToLocalMembersOnly(): void
    {
        $this->adapter->add(1, ['room']);
        $this->adapter->add(2, ['room']);
        $this->adapter->add(3, ['other']);

        $this->adapter->deliver($this->envelope('nodeB', ['room'], [], 'FRAME'));

        self::assertSame(['FRAME'], $this->sender->sent[1] ?? []);
        self::assertSame(['FRAME'], $this->sender->sent[2] ?? []);
        self::assertArrayNotHasKey(3, $this->sender->sent, 'a non-member must not receive');
    }

    #[Test]
    public function deliverHonoursExceptOnlyForSelfOriginatedFrames(): void
    {
        $this->adapter->add(1, ['room']);
        $this->adapter->add(2, ['room']);

        // Originated on THIS node → except fd 1 is meaningful and skipped.
        $this->adapter->deliver($this->envelope('nodeA', ['room'], [1], 'SELF'));
        self::assertArrayNotHasKey(1, $this->sender->sent, 'self-originated except must skip fd 1');
        self::assertSame(['SELF'], $this->sender->sent[2] ?? []);

        // Originated on ANOTHER node → its fd 1 is an unrelated connection there, so
        // our fd 1 must still receive.
        $this->adapter->deliver($this->envelope('nodeB', ['room'], [1], 'REMOTE'));
        self::assertSame(['REMOTE'], $this->sender->sent[1] ?? []);
        self::assertSame(['SELF', 'REMOTE'], $this->sender->sent[2] ?? []);
    }

    #[Test]
    public function deliverGlobalFrameReachesEveryLocalFd(): void
    {
        $this->adapter->add(1, []);
        $this->adapter->add(2, ['room']);

        $this->adapter->deliver($this->envelope('nodeB', [], [], 'ALL'));

        self::assertSame(['ALL'], $this->sender->sent[1] ?? []);
        self::assertSame(['ALL'], $this->sender->sent[2] ?? []);
    }

    #[Test]
    public function deliverStopsReachingAConnectionAfterDelAll(): void
    {
        $this->adapter->add(1, ['room']);
        $this->adapter->delAll(1);

        $this->adapter->deliver($this->envelope('nodeB', ['room'], [], 'FRAME'));

        self::assertSame([], $this->sender->sent);
    }

    #[Test]
    public function deliverIgnoresMalformedEnvelopes(): void
    {
        $this->adapter->add(1, ['room']);

        $this->adapter->deliver('not json at all');
        $this->adapter->deliver(json_encode(['node' => 'nodeB', 'rooms' => ['room']], JSON_THROW_ON_ERROR));

        self::assertSame([], $this->sender->sent, 'no frame → nothing delivered, no crash');
    }

    #[Test]
    public function broadcastEnvelopeRoundTripsThroughDeliver(): void
    {
        // The wire format broadcast() publishes must be exactly what deliver() consumes.
        $this->adapter->add(1, ['room']);
        $this->adapter->broadcast('WIRE', ['room'], []);

        $this->adapter->deliver($this->redis->published[0]['message']);

        self::assertSame(['WIRE'], $this->sender->sent[1] ?? []);
    }

    #[Test]
    public function publishStatsWritesASelfExpiringNodeScopedSnapshot(): void
    {
        $this->adapter->publishStats(['connections' => 42, 'messagesIn' => 100], 30);

        // {prefix}:stats:{nodeId} holds the JSON blob, TTL'd so a dead node's stats expire.
        self::assertSame('{"connections":42,"messagesIn":100}', $this->redis->strings['rt:stats:nodeA'] ?? null);
    }

    #[Test]
    public function clusterStatsAggregatesRegisteredReportingAndSilentNodes(): void
    {
        // nodeA (this adapter): registered + reporting.
        $this->adapter->heartbeatNode(30);
        $this->adapter->publishStats(['connections' => 10, 'messagesIn' => 5, 'pushesOk' => 50, 'pushesFailed' => 2], 30);

        // nodeB: a peer, registered + reporting.
        $this->redis->sAdd('rt:nodes', 'nodeB');
        $this->redis->setEx('rt:stats:nodeB', json_encode(
            ['connections' => 20, 'messagesIn' => 7, 'pushesOk' => 70, 'pushesFailed' => 1],
            JSON_THROW_ON_ERROR,
        ), 30);

        // nodeC: registered but NOT reporting (crashed / no snapshot yet).
        $this->redis->sAdd('rt:nodes', 'nodeC');

        $stats = $this->adapter->clusterStats();

        self::assertSame(3, $stats['totals']['nodes']);
        self::assertSame(2, $stats['totals']['reporting']);
        self::assertSame(30, $stats['totals']['connections']);   // 10 + 20
        self::assertSame(12, $stats['totals']['messagesIn']);    // 5 + 7
        self::assertSame(120, $stats['totals']['pushesOk']);     // 50 + 70
        self::assertSame(3, $stats['totals']['pushesFailed']);   // 2 + 1

        $byId = [];
        foreach ($stats['nodes'] as $node) {
            $byId[$node['nodeId']] = $node;
        }
        self::assertTrue($byId['nodeA']['reporting']);
        self::assertFalse($byId['nodeC']['reporting']);
        self::assertNull($byId['nodeC']['stats']);
    }

    #[Test]
    public function disconnectUserPublishesARevokeThatDropsOnlyThisNodesFdsForTheUser(): void
    {
        // nodeA owns fd 5 for user u1; a peer node (nodeB) owns fd 7 for the same user.
        $this->adapter->add(5, ['room'], ['id' => 'u1']);
        $this->redis->sAdd('rt:user:u1', 'nodeB:7');

        $this->adapter->disconnectUser('u1');

        // disconnectUser only PUBLISHES — the actual disconnect happens when each node delivers it.
        self::assertNotEmpty($this->redis->published, 'revoke is published cluster-wide');
        self::assertSame([], $this->sender->disconnected, 'no synchronous local disconnect');

        // Delivering the revoke on THIS node drops the fds THIS node owns for the user, not a peer's.
        $this->adapter->deliver($this->redis->published[array_key_last($this->redis->published)]['message']);

        $fds = array_column($this->sender->disconnected, 'fd');
        self::assertContains(5, $fds, "this node's fd for the user is disconnected");
        self::assertNotContains(7, $fds, "a peer node's fd is NOT disconnected here (node-local)");
    }

    /**
     * @param list<string> $rooms
     * @param list<int> $except
     */
    private function envelope(string $node, array $rooms, array $except, string $frame): string
    {
        return json_encode([
            'node' => $node,
            'rooms' => $rooms,
            'except' => $except,
            'frame' => $frame,
        ], JSON_THROW_ON_ERROR);
    }
}
