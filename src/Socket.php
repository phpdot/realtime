<?php

declare(strict_types=1);

/**
 * Socket — the per-connection API. Created by the Hub on open, destroyed on close.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime;

use Closure;
use PHPdot\Realtime\Contract\Adapter;
use Psr\Http\Message\ServerRequestInterface;

final class Socket
{
    /**
     * @var array<string, mixed>|null User identity (null until setUser).
     */
    private array|null $user = null;

    /**
     * @var array<string, list<Closure(mixed): void>> Registered event handlers.
     */
    private array $handlers = [];

    /**
     * @var list<Closure(): void> Disconnect handlers.
     */
    private array $disconnectHandlers = [];

    /**
     * One connected client: its file descriptor, upgrade request, and owning hub.
     *
     * @param int $fd The connection file descriptor.
     * @param ServerRequestInterface $request The WS upgrade request.
     * @param Hub $hub The Hub (for creating BroadcastOperators + adapter access).
     */
    public function __construct(
        private readonly int $fd,
        private readonly ServerRequestInterface $request,
        private readonly Hub $hub,
    ) {}

    /**
     * The connection file descriptor (connection ID).
     *
     * @return int
     */
    public function id(): int
    {
        return $this->fd;
    }

    /**
     * The authenticated user identity (null until setUser is called).
     *
     * @return array<string, mixed>|null
     */
    public function user(): array|null
    {
        return $this->user;
    }

    /**
     * Associate a user identity with this connection (after auth).
     *
     * @param array<string, mixed> $user
     *
     * @return void
     */
    public function setUser(array $user): void
    {
        $this->user = $user;
        $this->adapter()->add($this->fd, [], $user);
    }

    /**
     * The PSR-7 upgrade request (for extracting auth tokens, cookies, headers).
     *
     * @return ServerRequestInterface
     */
    public function request(): ServerRequestInterface
    {
        return $this->request;
    }

    /**
     * Send an event TO THIS socket only.
     *
     * @param string $event
     * @param mixed $payload
     *
     * @return void
     */
    public function emit(string $event, mixed $payload = null): void
    {
        $this->adapter()->send($this->fd, Event::encode($event, $payload));
    }

    /**
     * Returns a BroadcastOperator excluding THIS socket (explicit "exclude me").
     *
     * @return BroadcastOperator
     */
    public function broadcast(): BroadcastOperator
    {
        return (new BroadcastOperator($this->adapter()))->addExcept($this->fd);
    }

    /**
     * Returns a BroadcastOperator targeting room(s) (LITERAL — includes sender).
     *
     * @param string $rooms
     *
     * @return BroadcastOperator
     */
    public function to(string ...$rooms): BroadcastOperator
    {
        return (new BroadcastOperator($this->adapter()))->to(...$rooms);
    }

    /**
     * Returns a BroadcastOperator excluding specific fd(s).
     *
     * @param int $fds
     *
     * @return BroadcastOperator
     */
    public function except(int ...$fds): BroadcastOperator
    {
        return (new BroadcastOperator($this->adapter()))->except(...$fds);
    }

    /**
     * Returns a BroadcastOperator targeting ONE specific fd.
     *
     * @param int $fd
     *
     * @return BroadcastOperator
     */
    public function direct(int $fd): BroadcastOperator
    {
        return (new BroadcastOperator($this->adapter()))->setTargetFd($fd);
    }

    /**
     * Join room(s) as a PRESENCE channel — triggers presence:here/joined/left (chat).
     *
     * @param string $rooms
     *
     * @return void
     */
    public function join(string ...$rooms): void
    {
        $this->hub->joinRooms($this->fd, array_values($rooms), $this->user);
    }

    /**
     * Leave presence room(s). Triggers presence:left.
     *
     * @param string $rooms
     *
     * @return void
     */
    public function leave(string ...$rooms): void
    {
        $this->hub->leaveRooms($this->fd, array_values($rooms), $this->user);
    }

    /**
     * Subscribe to room(s) for BROADCAST ONLY — pub/sub, NO presence (one-way feeds).
     *
     * @param string $rooms
     *
     * @return void
     */
    public function subscribe(string ...$rooms): void
    {
        $this->hub->subscribeRooms($this->fd, array_values($rooms), $this->user);
    }

    /**
     * Unsubscribe from silent room(s) — counterpart to subscribe(), no presence.
     *
     * @param string $rooms
     *
     * @return void
     */
    public function unsubscribe(string ...$rooms): void
    {
        $this->hub->unsubscribeRooms($this->fd, array_values($rooms));
    }

    /**
     * Register a handler for an event from this client.
     *
     * @param Closure $handler
     * @param string $event
     *
     * @return void
     */
    public function on(string $event, Closure $handler): void
    {
        $this->handlers[$event][] = $handler;
    }

    /**
     * Register a handler fired when this socket disconnects.
     *
     * @param Closure $handler
     *
     * @return void
     */
    public function onDisconnect(Closure $handler): void
    {
        $this->disconnectHandlers[] = $handler;
    }

    /**
     * Force-close this connection.
     *
     * @param int $code
     * @param string $reason
     *
     * @return void
     */
    public function disconnect(int $code = 1000, string $reason = ''): void
    {
        $this->hub->disconnectFd($this->fd, $code, $reason);
    }

    /**
     * Dispatch an incoming event to registered handlers.
     *
     * @param string $event
     * @param mixed $payload
     *
     * @return void
     */
    public function dispatch(string $event, mixed $payload): void
    {
        foreach ($this->handlers[$event] ?? [] as $handler) {
            $handler($payload);
        }
    }

    /**
     * Fire all registered disconnect handlers.
     *
     * @return void
     */
    public function fireDisconnect(): void
    {
        foreach ($this->disconnectHandlers as $handler) {
            $handler();
        }
    }

    /**
     * The adapter this socket's hub broadcasts through.
     *
     * @return Adapter
     */
    private function adapter(): Adapter
    {
        return $this->hub->adapter();
    }
}
