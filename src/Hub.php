<?php

declare(strict_types=1);

/**
 * Hub — the server-wide WebSocket hub (the io). Manages connections, rooms,
 * presence, and broadcast routing. Transport-agnostic: it reaches clients only
 * through a ConnectionSenderInterface, never a concrete server.
 *
 * The Hub is driven by the transport's WebSocket handler via
 * handleOpen/handleMessage/handleClose. On open → creates a Socket, fires
 * onConnection handlers. On message → decodes the JSON frame and fires the
 * socket's on() handlers (channel routing is a Router concern, layered above).
 * On close → cleanup + presence:left.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime;

use Closure;
use PHPdot\Contracts\Server\ConnectionSenderInterface;
use PHPdot\Realtime\Contract\Adapter;
use Psr\Http\Message\ServerRequestInterface;

final class Hub
{
    /**
     * @var array<int, Socket> fd→Socket map (this instance's connections).
     */
    private array $sockets = [];

    /**
     * @var list<Closure(Socket): void> Connection handlers.
     */
    private array $onConnectionHandlers = [];

    /**
     * fd → rooms it joined WITH presence (via join()). Silent pub/sub subscriptions
     * (subscribe()) are deliberately absent, so close() emits presence:left ONLY for
     * these. Per-worker, same locality as $sockets (an fd's open/close share a worker).
     *
     * @var array<int, list<string>>
     */
    private array $presenceRooms = [];

    /**
     * fd → last time (microtime) a frame was received FROM the client. Refreshed on
     * every inbound frame (incl. PONGs via touch()); heartbeat() reaps fds idle past
     * the timeout. Per-worker, same locality as $sockets.
     *
     * @var array<int, float>
     */
    private array $lastSeen = [];

    /**
     * Track sockets and rooms and fan events out through the adapter.
     *
     * @param Adapter $adapter Room/presence backend (Table or Redis).
     * @param ConnectionSenderInterface $sender Pushes frames to a local connection by fd.
     */
    public function __construct(
        private readonly Adapter $adapter,
        private readonly ConnectionSenderInterface $sender,
    ) {}

    /**
     * The adapter (for Socket/BroadcastOperator to access).
     *
     * @return Adapter
     */
    public function adapter(): Adapter
    {
        return $this->adapter;
    }

    /**
     * Called on WS open — creates a Socket, fires onConnection.
     *
     * @param int $fd
     * @param ServerRequestInterface $request
     *
     * @return void
     */
    public function handleOpen(int $fd, ServerRequestInterface $request): void
    {
        $socket = new Socket($fd, $request, $this);
        $this->sockets[$fd] = $socket;
        $this->lastSeen[$fd] = microtime(true);

        $this->adapter->add($fd, []);

        foreach ($this->onConnectionHandlers as $handler) {
            $handler($socket);
        }
    }

    /**
     * Called on WS message — fires the socket's on() handlers. Channel dispatch is a Router concern.
     *
     * @param string $data
     * @param int $fd
     *
     * @return void
     */
    public function handleMessage(int $fd, string $data): void
    {
        $socket = $this->sockets[$fd] ?? null;

        if ($socket === null) {
            return;
        }

        $this->touch($fd);

        $decoded = Event::decode($data);

        if ($decoded === null) {
            return;
        }

        $socket->dispatch($decoded['event'], $decoded['data']);
    }

    /**
     * Called on WS close — fires disconnect handlers, cleans up adapter, presence:left.
     *
     * @param int $fd
     *
     * @return void
     */
    public function handleClose(int $fd): void
    {
        $socket = $this->sockets[$fd] ?? null;

        if ($socket !== null) {
            $socket->fireDisconnect();
        }

        $user = $this->adapter->userOf($fd);

        foreach ($this->presenceRooms[$fd] ?? [] as $room) {
            $this->adapter->broadcast(
                Event::encode('presence:left', ['user' => $user, 'fd' => $fd, 'room' => $room]),
                [$room],
                [$fd],
            );
        }

        $this->adapter->delAll($fd);

        unset($this->presenceRooms[$fd], $this->lastSeen[$fd], $this->sockets[$fd]);
    }

    /**
     * Refresh an fd's last-seen time — any inbound frame (including a PONG) means the
     * client is alive. The transport handler calls this on every received WS frame.
     *
     * @param float|null $at Timestamp to record (microtime); defaults to now. Injectable for tests.
     * @param int $fd
     *
     * @return void
     */
    public function touch(int $fd, ?float $at = null): void
    {
        if (isset($this->sockets[$fd])) {
            $this->lastSeen[$fd] = $at ?? microtime(true);
        }
    }

    /**
     * Heartbeat sweep — run on a timer, per worker. PINGs every recently-seen connection
     * (eliciting a PONG that refreshes last-seen) and disconnects any idle longer than
     * $idleTimeout seconds — a vanished client that stopped answering. The disconnect
     * flows through the transport's close event → handleClose() cleanup. This is the only
     * reliable way to reap a dead PUSH connection (TCP keepalive never fires when the
     * server is actively pushing; verified via a network-partition test).
     *
     * @param float $idleTimeout Seconds without an inbound frame before a connection is reaped.
     * @param float|null $now Current time (microtime); injectable for tests.
     *
     * @return int Number of connections reaped.
     */
    public function heartbeat(float $idleTimeout, ?float $now = null): int
    {
        $now ??= microtime(true);
        $stale = [];

        foreach (array_keys($this->sockets) as $fd) {
            if ($now - ($this->lastSeen[$fd] ?? $now) > $idleTimeout) {
                $stale[] = $fd;
            } else {
                $this->sender->pingWs($fd);
            }
        }

        foreach ($stale as $fd) {
            $this->sender->disconnect($fd, 1001, 'heartbeat timeout');
        }

        return count($stale);
    }

    /**
     * Register a handler fired when a new WS connection opens.
     *
     * @param Closure $handler
     *
     * @return void
     */
    public function onConnection(Closure $handler): void
    {
        $this->onConnectionHandlers[] = $handler;
    }

    /**
     * Create a BroadcastOperator targeting room(s). to() is LITERAL (full room).
     *
     * @param string $rooms
     *
     * @return BroadcastOperator
     */
    public function to(string ...$rooms): BroadcastOperator
    {
        return (new BroadcastOperator($this->adapter))->to(...$rooms);
    }

    /**
     * Create a BroadcastOperator excluding specific fd(s).
     *
     * @param int $fds
     *
     * @return BroadcastOperator
     */
    public function except(int ...$fds): BroadcastOperator
    {
        return (new BroadcastOperator($this->adapter))->except(...$fds);
    }

    /**
     * Create a BroadcastOperator targeting ONE specific fd (direct/private message).
     *
     * @param int $fd
     *
     * @return BroadcastOperator
     */
    public function direct(int $fd): BroadcastOperator
    {
        return (new BroadcastOperator($this->adapter))->setTargetFd($fd);
    }

    /**
     * Broadcast to ALL connected WS clients.
     *
     * @param string $event
     * @param mixed $payload
     *
     * @return void
     */
    public function emit(string $event, mixed $payload = null): void
    {
        $this->adapter->broadcast(Event::encode($event, $payload), [], []);
    }

    /**
     * Get a RoomFacade for presence queries.
     *
     * @param string $name
     *
     * @return RoomFacade
     */
    public function room(string $name): RoomFacade
    {
        return new RoomFacade($this->adapter, $name);
    }

    /**
     * Fetch a Socket by fd (or null if not connected to this instance).
     *
     * @param int $fd
     *
     * @return ?Socket
     */
    public function socket(int $fd): ?Socket
    {
        return $this->sockets[$fd] ?? null;
    }

    /**
     * Every socket this worker owns — for periodic sweeps (e.g. re-validating auth). Per-worker
     * under sticky dispatch, so a sweep on each worker covers all of a node's connections.
     *
     * @return list<Socket>
     */
    public function localSockets(): array
    {
        return array_values($this->sockets);
    }

    /**
     * Force-disconnect every socket belonging to a user, CLUSTER-WIDE (all nodes). The adapter
     * relays the revoke: single-node drops local fds directly; multi-node publishes so a user's
     * connections on other nodes are dropped too. Used for logout / session revocation.
     *
     * @param string $userId
     *
     * @return void
     */
    public function disconnectUser(string $userId): void
    {
        $this->adapter->disconnectUser($userId);
    }

    /**
     * Join an fd to rooms + fire presence events.
     *
     * @param list<string> $rooms
     * @param array<string, mixed>|null $user
     * @param int $fd
     *
     * @return void
     */
    public function joinRooms(int $fd, array $rooms, ?array $user): void
    {
        foreach ($rooms as $room) {
            $members = $this->adapter->members([$room]);
            $roster = array_map(static fn(array $m): ?array => $m['user'], $members);
            $this->adapter->send($fd, Event::encode('presence:here', ['members' => $roster, 'room' => $room]));

            $this->adapter->add($fd, [$room], $user);

            $this->adapter->broadcast(
                Event::encode('presence:joined', ['user' => $user, 'fd' => $fd, 'room' => $room]),
                [$room],
                [$fd],
            );
        }

        $this->presenceRooms[$fd] = array_values(array_unique([...($this->presenceRooms[$fd] ?? []), ...$rooms]));
    }

    /**
     * Subscribe an fd to room(s) for BROADCAST ONLY — pub/sub, NO presence. Use for
     * one-way feeds (a price ticker) where "who's here" is meaningless: the fd joins
     * the room so broadcasts reach it, but emits no presence:here/joined/left.
     *
     * @param list<string> $rooms
     * @param array<string, mixed>|null $user
     * @param int $fd
     *
     * @return void
     */
    public function subscribeRooms(int $fd, array $rooms, ?array $user): void
    {
        $this->adapter->add($fd, $rooms, $user);
    }

    /**
     * Unsubscribe an fd from silent room(s) — the counterpart to subscribeRooms, no presence.
     *
     * @param list<string> $rooms
     * @param int $fd
     *
     * @return void
     */
    public function unsubscribeRooms(int $fd, array $rooms): void
    {
        $this->adapter->del($fd, $rooms);
    }

    /**
     * Remove an fd from rooms + fire presence:left.
     *
     * @param list<string> $rooms
     * @param array<string, mixed>|null $user
     * @param int $fd
     *
     * @return void
     */
    public function leaveRooms(int $fd, array $rooms, ?array $user): void
    {
        foreach ($rooms as $room) {
            $this->adapter->broadcast(
                Event::encode('presence:left', ['user' => $user, 'fd' => $fd, 'room' => $room]),
                [$room],
                [$fd],
            );

            $this->adapter->del($fd, [$room]);
        }

        if (isset($this->presenceRooms[$fd])) {
            $this->presenceRooms[$fd] = array_values(array_diff($this->presenceRooms[$fd], $rooms));
        }
    }

    /**
     * Disconnect an fd (force-close) through the sender seam.
     *
     * @param int $code
     * @param string $reason
     * @param int $fd
     *
     * @return void
     */
    public function disconnectFd(int $fd, int $code, string $reason): void
    {
        $this->sender->disconnect($fd, $code, $reason);
    }
}
