<?php

declare(strict_types=1);

/**
 * RedisAdapter — the MULTI-NODE adapter. Membership, presence and identity live in
 * Redis (shared across every server instance), and broadcasts relay between nodes over
 * Redis pub/sub: a node PUBLISHes to the broadcast channel and each node's
 * {@see RedisSubscriber} fans the frame out to ITS OWN local connections (a node can
 * only push to its own fds). Connections are identified globally as "{nodeId}:{fd}".
 *
 * Redis key schema (prefix default "rt"):
 * - {p}:conn:{node}:{fd}  HASH {rooms(json), userId, userData}  — one per connection
 * - {p}:room:{room}       HASH {"{node}:{fd}" => userJson}      — room membership + roster
 * - {p}:user:{userId}     SET  {"{node}:{fd}"}                  — a user's connections
 * - {p}:broadcast         pub/sub channel                       — the broadcast relay
 * - {p}:nodes             SET  {nodeId}                         — cluster node registry
 * - {p}:node:{node}       string, TTL                          — a node's liveness heartbeat
 * - {p}:nodeconns:{node}  SET  {fd}                            — a node's conns, for reaping
 * - {p}:reaping:{node}    string, TTL (NX)                     — single-winner reap lock
 *
 * Fan-out is uncapped (Redis holds membership, so the single-node ~1000/room blob cap is
 * gone) and O(local): {@see deliver()} pushes from a per-worker in-memory index, never
 * reading Redis on the hot path. Ungraceful node death leaks membership (no delAll runs),
 * so {@see heartbeatNode()} + {@see reap()} publish per-node liveness and reap the
 * membership of nodes whose heartbeat has expired — see those methods. count() uses HLEN
 * (O(1)) for the common single-room case. Still open: a members() roster cap for very
 * large rooms, and cross-node one-to-one routing (direct/disconnectUser).
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Adapter;

use Closure;
use PHPdot\Contracts\Server\ConnectionSenderInterface;
use PHPdot\Realtime\Contract\MultiNodeAdapter;
use PHPdot\Realtime\Contract\RedisCommands;

final class RedisAdapter implements MultiNodeAdapter
{
    /**
     * Connections cleaned per reap batch before pausing to pace Redis under a huge dead node.
     */
    private const int REAP_BATCH = 500;

    /**
     * Pub/sub envelope kind: revoke a user's connections cluster-wide (vs. a normal message relay).
     */
    private const string KIND_DISCONNECT_USER = 'disconnect-user';

    /**
     * Every local fd owned by THIS worker — the target set for a global broadcast.
     *
     * @var array<int, true>
     */
    private array $localFds = [];

    /**
     * room => set of THIS worker's local fds in it — the fan-out index. Redis holds
     * the global roster; this holds only what this worker can actually push to, so
     * fan-out never reads Redis on the hot path and never double-delivers (each fd is
     * owned by exactly one worker under sticky dispatch).
     *
     * @var array<string, array<int, true>>
     */
    private array $localRooms = [];

    /**
     * Back rooms, presence, and cross-node broadcast relay with Redis.
     *
     * @param Closure(): RedisCommands $connect Yields a coroutine-safe command connection.
     * @param ConnectionSenderInterface $sender Pushes frames to a local connection by fd.
     * @param string $nodeId Unique per server instance (must not contain ':').
     * @param string $prefix Key prefix namespacing this cluster's Redis keys.
     */
    public function __construct(
        private readonly Closure $connect,
        private readonly ConnectionSenderInterface $sender,
        private readonly string $nodeId,
        private readonly string $prefix = 'rt',
    ) {}

    public function add(int $fd, array $rooms, ?array $user = null): void
    {
        $redis = $this->redis();
        $member = $this->member($fd);
        $connKey = $this->connKey($fd);
        $conn = $redis->hGetAll($connKey);

        $current = $this->decodeStringList($conn['rooms'] ?? '');
        $merged = array_values(array_unique([...$current, ...$rooms]));

        $userId = $conn['userId'] ?? '';
        $userData = $conn['userData'] ?? '';
        if ($user !== null) {
            $rawId = $user['id'] ?? '';
            $userId = is_scalar($rawId) ? (string) $rawId : '';
            $userData = json_encode($user, JSON_THROW_ON_ERROR);
        }

        $redis->hSet($connKey, 'rooms', json_encode($merged, JSON_THROW_ON_ERROR));
        $redis->hSet($connKey, 'userId', $userId);
        $redis->hSet($connKey, 'userData', $userData);

        $memberUser = $userData !== '' ? $userData : 'null';
        foreach ($rooms as $room) {
            $redis->hSet($this->roomKey($room), $member, $memberUser);
        }

        if ($userId !== '') {
            $redis->sAdd($this->userKey($userId), $member);
        }

        $redis->sAdd($this->nodeConnsKey($this->nodeId), (string) $fd);

        $this->localFds[$fd] = true;
        foreach ($rooms as $room) {
            $this->localRooms[$room][$fd] = true;
        }
    }

    public function del(int $fd, array $rooms): void
    {
        $redis = $this->redis();
        $member = $this->member($fd);

        foreach ($rooms as $room) {
            $redis->hDel($this->roomKey($room), $member);
        }

        $connKey = $this->connKey($fd);
        $current = $this->decodeStringList($redis->hGet($connKey, 'rooms') ?? '');
        $remaining = array_values(array_diff($current, $rooms));
        $redis->hSet($connKey, 'rooms', json_encode($remaining, JSON_THROW_ON_ERROR));

        foreach ($rooms as $room) {
            unset($this->localRooms[$room][$fd]);
            if (($this->localRooms[$room] ?? []) === []) {
                unset($this->localRooms[$room]);
            }
        }
    }

    public function delAll(int $fd): void
    {
        $redis = $this->redis();
        $member = $this->member($fd);
        $connKey = $this->connKey($fd);
        $conn = $redis->hGetAll($connKey);

        if ($conn === []) {
            return;
        }

        foreach ($this->decodeStringList($conn['rooms'] ?? '') as $room) {
            $redis->hDel($this->roomKey($room), $member);
            unset($this->localRooms[$room][$fd]);
            if (($this->localRooms[$room] ?? []) === []) {
                unset($this->localRooms[$room]);
            }
        }

        $userId = $conn['userId'] ?? '';
        if ($userId !== '') {
            $redis->sRem($this->userKey($userId), $member);
        }

        $redis->del($connKey);
        $redis->sRem($this->nodeConnsKey($this->nodeId), (string) $fd);
        unset($this->localFds[$fd]);
    }

    public function broadcast(string $jsonFrame, array $rooms, array $exceptFds): void
    {
        $this->redis()->publish($this->broadcastChannel(), json_encode([
            'node' => $this->nodeId,
            'rooms' => $rooms,
            'except' => $exceptFds,
            'frame' => $jsonFrame,
        ], JSON_THROW_ON_ERROR));
    }

    public function disconnectUser(string $userId): void
    {
        $this->redis()->publish($this->broadcastChannel(), json_encode([
            'node' => $this->nodeId,
            'kind' => self::KIND_DISCONNECT_USER,
            'user' => $userId,
        ], JSON_THROW_ON_ERROR));
    }

    /**
     * The pub/sub channel the broadcast relay publishes to and {@see RedisSubscriber} listens on.
     *
     * @return string
     */
    public function broadcastChannel(): string
    {
        return $this->prefix . ':broadcast';
    }

    /**
     * Fan a relayed broadcast envelope out to THIS node's local connections. Called by
     * {@see RedisSubscriber} for every message on the broadcast channel — INCLUDING
     * frames this node itself published. Delivery is uniform (publish-only, no
     * synchronous same-node fast path, which would double-deliver, since the origin
     * worker owns only some of the node's fds).
     *
     * The envelope's `except` fds are honoured ONLY when the frame originated on this
     * node: fd numbers are per-node, so another node's fd 3 is an unrelated local
     * connection here.
     *
     * @param string $envelope
     *
     * @return void
     */
    public function deliver(string $envelope): void
    {
        $decoded = json_decode($envelope, true);
        if (!is_array($decoded)) {
            return;
        }

        if (($decoded['kind'] ?? null) === self::KIND_DISCONNECT_USER) {
            $userId = $decoded['user'] ?? null;
            if (is_string($userId)) {
                foreach ($this->fdsOfUser($userId) as $fd) {
                    if (isset($this->localFds[$fd])) {
                        $this->sender->disconnect($fd, 1000, 'session revoked');
                    }
                }
            }

            return;
        }

        $frame = $decoded['frame'] ?? null;
        if (!is_string($frame)) {
            return;
        }

        $rooms = $this->stringListFrom($decoded['rooms'] ?? []);
        $fromSelf = ($decoded['node'] ?? null) === $this->nodeId;
        $except = $fromSelf ? $this->intSetFrom($decoded['except'] ?? []) : [];

        $targets = $rooms === [] ? $this->localFds : $this->localFdsInRooms($rooms);

        foreach ($targets as $fd => $_) {
            if (isset($except[$fd])) {
                continue;
            }
            $this->sender->pushWs($fd, $frame);
        }
    }

    public function send(int $fd, string $jsonFrame): void
    {
        $this->sender->pushWs($fd, $jsonFrame);
    }

    public function members(array $rooms): array
    {
        $redis = $this->redis();
        $result = [];
        $seen = [];

        foreach ($rooms as $room) {
            foreach ($redis->hGetAll($this->roomKey($room)) as $member => $userJson) {
                if (isset($seen[$member])) {
                    continue;
                }
                $seen[$member] = true;
                $decoded = json_decode($userJson, true);
                $result[] = ['fd' => $this->fdOf($member), 'user' => is_array($decoded) ? $decoded : null];
            }
        }

        return $result;
    }

    public function count(array $rooms): int
    {
        $redis = $this->redis();
        $seen = [];
        $multi = count($rooms) > 1;

        foreach ($rooms as $room) {
            if (!$multi) {
                return $redis->hLen($this->roomKey($room));
            }
            foreach (array_keys($redis->hGetAll($this->roomKey($room))) as $member) {
                $seen[$member] = true;
            }
        }

        return count($seen);
    }

    public function userOf(int $fd): ?array
    {
        $userData = $this->redis()->hGet($this->connKey($fd), 'userData') ?? '';
        if ($userData === '') {
            return null;
        }
        $decoded = json_decode($userData, true);

        return is_array($decoded) ? $decoded : null;
    }

    public function fdsOfUser(string $userId): array
    {
        $fds = [];
        foreach ($this->redis()->sMembers($this->userKey($userId)) as $member) {
            if ($this->nodeOf($member) === $this->nodeId) {
                $fds[] = $this->fdOf($member);
            }
        }

        return $fds;
    }

    public function roomsOf(int $fd): array
    {
        return $this->decodeStringList($this->redis()->hGet($this->connKey($fd), 'rooms') ?? '');
    }

    /**
     * Publish this node's liveness (a TTL key refreshed on a timer) and register it in
     * the cluster node set. Sets the liveness key BEFORE joining the registry, so a peer
     * reaper can never see this node listed without a heartbeat and reap it mid-boot.
     * Call once on start and on every heartbeat tick; $ttlSeconds should be a few times
     * the tick interval so a couple of missed refreshes don't declare the node dead.
     */
    public function heartbeatNode(int $ttlSeconds): void
    {
        $redis = $this->redis();
        $redis->setEx($this->nodeKey($this->nodeId), '1', $ttlSeconds);
        $redis->sAdd($this->nodesKey(), $this->nodeId);
    }

    public function publishStats(array $stats, int $ttlSeconds): void
    {
        $this->redis()->setEx($this->statsKey($this->nodeId), json_encode($stats, JSON_THROW_ON_ERROR), $ttlSeconds);
    }

    public function clusterStats(): array
    {
        $redis = $this->redis();
        $nodes = [];
        $totals = ['nodes' => 0, 'reporting' => 0, 'connections' => 0, 'messagesIn' => 0, 'pushesOk' => 0, 'pushesFailed' => 0];

        foreach ($redis->sMembers($this->nodesKey()) as $nodeId) {
            $totals['nodes']++;
            $stats = $this->decodeStats($redis->get($this->statsKey($nodeId)));
            if ($stats !== null) {
                $totals['reporting']++;
                $totals['connections'] += $this->statInt($stats, 'connections');
                $totals['messagesIn'] += $this->statInt($stats, 'messagesIn');
                $totals['pushesOk'] += $this->statInt($stats, 'pushesOk');
                $totals['pushesFailed'] += $this->statInt($stats, 'pushesFailed');
            }
            $nodes[] = ['nodeId' => $nodeId, 'reporting' => $stats !== null, 'stats' => $stats];
        }

        return ['totals' => $totals, 'nodes' => $nodes];
    }

    /**
     * Decode a node's stats blob, keeping only scalar fields. Null if absent (not reporting)
     * or malformed.
     *
     * @param ?string $json
     *
     * @return array<string, int|float|string>|null
     */
    private function decodeStats(?string $json): ?array
    {
        if ($json === null) {
            return null;
        }

        $decoded = json_decode($json, true);
        if (!is_array($decoded)) {
            return null;
        }

        $stats = [];
        foreach ($decoded as $key => $value) {
            if (is_string($key) && (is_int($value) || is_float($value) || is_string($value))) {
                $stats[$key] = $value;
            }
        }

        return $stats;
    }

    /**
     * Read one integer stat from a stats map, defaulting to 0.
     *
     * @param array<string, int|float|string> $stats
     * @param string $key
     *
     * @return int
     */
    private function statInt(array $stats, string $key): int
    {
        $value = $stats[$key] ?? 0;

        return is_int($value) ? $value : 0;
    }

    /**
     * Reap every DEAD node's leaked membership. A node is dead when it is in the registry
     * but its liveness key has expired (ungraceful exit — a crash leaves no delAll behind).
     * Runs on a timer on EVERY node, coordinated by a per-dead-node NX lock so each is
     * reaped once cluster-wide; never reaps self. Because nodeId is per-incarnation, a
     * crash-and-restart is a NEW node and the crashed id's entries reap normally.
     *
     * A false reap (a live node whose liveness lapsed on a Redis/network blip) is bounded:
     * ROOM-BROADCAST delivery survives (deliver() fans out from the per-worker in-memory
     * index, which this never touches), but USER-TARGETED ops (fdsOfUser, and any
     * direct-to-user/disconnectUser built on it) read Redis and go blind to that node's
     * users until they rejoin. A generous TTL and the under-lock re-check keep it rare.
     *
     * @param Closure(): void|null $yield Called between batches to pace Redis under a huge dead node.
     *
     * @return int Connections reaped.
     */
    public function reap(int $lockTtlSeconds = 30, ?Closure $yield = null): int
    {
        $redis = $this->redis();
        $reaped = 0;

        foreach ($redis->sMembers($this->nodesKey()) as $nodeId) {
            if ($nodeId === $this->nodeId || !$this->nodeIsDead($redis, $nodeId)) {
                continue;
            }

            if (!$redis->setNx($this->reapLockKey($nodeId), $this->nodeId, $lockTtlSeconds)) {
                continue;
            }
            if (!$this->nodeIsDead($redis, $nodeId)) {
                continue;
            }

            $reaped += $this->reapNode($redis, $nodeId, $yield);
        }

        return $reaped;
    }

    /**
     * A node is dead when its liveness heartbeat key has expired. Impure: it reads
     * mutable Redis state, so two calls a moment apart can legitimately differ (a node
     * revives) — which is exactly why reap() checks it again under the lock.
     *
     * @phpstan-impure
     *
     * @param RedisCommands $redis
     * @param string $nodeId
     *
     * @return bool
     */
    private function nodeIsDead(RedisCommands $redis, string $nodeId): bool
    {
        return !$redis->exists($this->nodeKey($nodeId));
    }

    /**
     * Remove a dead node's rooms, presence, and connection records; return how many were reaped.
     *
     * @param RedisCommands $redis Coroutine-safe command connection
     * @param string $nodeId The dead node's id
     * @param (Closure(): void)|null $yield Called between batches to pace Redis under a large dead node
     *
     * @return int
     */
    private function reapNode(RedisCommands $redis, string $nodeId, ?Closure $yield): int
    {
        $connsKey = $this->nodeConnsKey($nodeId);
        $count = 0;

        foreach ($redis->sMembers($connsKey) as $fd) {
            $member = $nodeId . ':' . $fd;
            $connKey = $this->prefix . ':conn:' . $member;
            $conn = $redis->hGetAll($connKey);

            foreach ($this->decodeStringList($conn['rooms'] ?? '') as $room) {
                $redis->hDel($this->roomKey($room), $member);
            }

            $userId = $conn['userId'] ?? '';
            if ($userId !== '') {
                $redis->sRem($this->userKey($userId), $member);
            }

            $redis->del($connKey);

            if (++$count % self::REAP_BATCH === 0 && $yield !== null) {
                $yield();
            }
        }

        $redis->del($connsKey);
        $redis->sRem($this->nodesKey(), $nodeId);

        return $count;
    }

    /**
     * Borrow a coroutine-safe Redis command connection.
     *
     * @return RedisCommands
     */
    private function redis(): RedisCommands
    {
        return ($this->connect)();
    }

    /**
     * Union of this worker's local fds across the given rooms.
     *
     * @param list<string> $rooms
     *
     * @return array<int, true>
     */
    private function localFdsInRooms(array $rooms): array
    {
        if (count($rooms) === 1) {
            return $this->localRooms[$rooms[0]] ?? [];
        }

        $targets = [];
        foreach ($rooms as $room) {
            foreach ($this->localRooms[$room] ?? [] as $fd => $_) {
                $targets[$fd] = true;
            }
        }

        return $targets;
    }

    /**
     * Coerce a raw Redis reply into a list of strings.
     *
     * @param mixed $value
     *
     * @return list<string>
     */
    private function stringListFrom(mixed $value): array
    {
        if (!is_array($value)) {
            return [];
        }

        $result = [];
        foreach ($value as $item) {
            if (is_string($item)) {
                $result[] = $item;
            }
        }

        return $result;
    }

    /**
     * Coerce a raw Redis reply into a set (unique keys) of ints.
     *
     * @param mixed $value
     *
     * @return array<int, true>
     */
    private function intSetFrom(mixed $value): array
    {
        if (!is_array($value)) {
            return [];
        }

        $result = [];
        foreach ($value as $item) {
            if (is_int($item)) {
                $result[$item] = true;
            }
        }

        return $result;
    }

    /**
     * Global connection id: "{nodeId}:{fd}".
     *
     * @param int $fd
     *
     * @return string
     */
    private function member(int $fd): string
    {
        return $this->nodeId . ':' . $fd;
    }

    /**
     * Node of.
     *
     * @param string $member
     *
     * @return string
     */
    private function nodeOf(string $member): string
    {
        $pos = strrpos($member, ':');

        return $pos === false ? '' : substr($member, 0, $pos);
    }

    /**
     * Fd of.
     *
     * @param string $member
     *
     * @return int
     */
    private function fdOf(string $member): int
    {
        $pos = strrpos($member, ':');

        return $pos === false ? 0 : (int) substr($member, $pos + 1);
    }

    /**
     * Conn key.
     *
     * @param int $fd
     *
     * @return string
     */
    private function connKey(int $fd): string
    {
        return $this->prefix . ':conn:' . $this->nodeId . ':' . $fd;
    }

    /**
     * Room key.
     *
     * @param string $room
     *
     * @return string
     */
    private function roomKey(string $room): string
    {
        return $this->prefix . ':room:' . $room;
    }

    /**
     * User key.
     *
     * @param string $userId
     *
     * @return string
     */
    private function userKey(string $userId): string
    {
        return $this->prefix . ':user:' . $userId;
    }

    /**
     * SET of every registered nodeId in the cluster.
     *
     * @return string
     */
    private function nodesKey(): string
    {
        return $this->prefix . ':nodes';
    }

    /**
     * A node's liveness key (TTL heartbeat).
     *
     * @param string $nodeId
     *
     * @return string
     */
    private function nodeKey(string $nodeId): string
    {
        return $this->prefix . ':node:' . $nodeId;
    }

    /**
     * SET of the fds a node owns — the reaper's enumeration index.
     *
     * @param string $nodeId
     *
     * @return string
     */
    private function nodeConnsKey(string $nodeId): string
    {
        return $this->prefix . ':nodeconns:' . $nodeId;
    }

    /**
     * Single-winner lock so a dead node is reaped once cluster-wide.
     *
     * @param string $nodeId
     *
     * @return string
     */
    private function reapLockKey(string $nodeId): string
    {
        return $this->prefix . ':reaping:' . $nodeId;
    }

    /**
     * A node's self-expiring stats snapshot key.
     *
     * @param string $nodeId
     *
     * @return string
     */
    private function statsKey(string $nodeId): string
    {
        return $this->prefix . ':stats:' . $nodeId;
    }

    /**
     * Decode a JSON array into a list of strings; empty on malformed input.
     *
     * @param string $json
     *
     * @return list<string>
     */
    private function decodeStringList(string $json): array
    {
        if ($json === '') {
            return [];
        }

        $decoded = json_decode($json, true);
        if (!is_array($decoded)) {
            return [];
        }

        $result = [];
        foreach ($decoded as $value) {
            if (is_string($value)) {
                $result[] = $value;
            } elseif (is_int($value)) {
                $result[] = (string) $value;
            }
        }

        return $result;
    }
}
