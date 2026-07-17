<?php

declare(strict_types=1);

/**
 * TableAdapter — the SINGLE-NODE adapter, backed by Swoole\Table (shared memory,
 * cross-worker within one instance). Reaches clients through the sender seam, so
 * it never names a concrete server.
 *
 * MUST be constructed BEFORE the server forks its workers (Swoole\Table is only
 * shared memory if created pre-fork) — i.e. resolve it once at bootstrap.
 *
 * KNOWN SINGLE-NODE LIMITS (Swoole\Table stores each membership set as a JSON blob
 * in a fixed-width string column; oversized writes are truncated, not errored):
 * - `rooms.members` caps a room's presence roster — with ~100-byte identities the
 * default width holds a few hundred members; beyond that the roster truncates.
 * Tune the widths below for larger rooms, or move to the multi-node RedisAdapter.
 * - Membership mutations are get-modify-set on one row (not atomic across the
 * read+write), so two coroutines joining the SAME room concurrently can drop one
 * write. Fine for human-paced rooms; a hot fan-in room needs the RedisAdapter.
 * - Presence dedups by fd, so one user across N tabs appears N times.
 *
 * For multi-node (rooms/presence across instances), a RedisAdapter implementing
 * this same interface is the intended path (see README) — it is not yet built, so
 * this package is single-node for v1.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Adapter;

use PHPdot\Contracts\Server\ConnectionSenderInterface;
use PHPdot\Realtime\Contract\Adapter;
use Swoole\Table;

final class TableAdapter implements Adapter
{
    private Table $connections;

    private Table $users;

    private Table $rooms;

    /**
     * Back rooms and presence for a single node in shared Swoole tables.
     *
     * @param int $maxConnections Table capacity for connections + users.
     * @param int $maxRooms Table capacity for rooms.
     * @param int $roomMembersBytes Width of a room's presence-roster JSON blob (caps members/room).
     * @param ConnectionSenderInterface $sender Pushes frames to a local connection by fd.
     */
    public function __construct(
        private readonly ConnectionSenderInterface $sender,
        int $maxConnections = 10000,
        int $maxRooms = 5000,
        int $roomMembersBytes = 16384,
    ) {
        $this->connections = new Table($maxConnections);
        $this->connections->column('userId', Table::TYPE_STRING, 64);
        $this->connections->column('userData', Table::TYPE_STRING, 1024);
        $this->connections->column('rooms', Table::TYPE_STRING, 4096);
        $this->connections->create();

        $this->users = new Table($maxConnections);
        $this->users->column('fds', Table::TYPE_STRING, 2048);
        $this->users->create();

        $this->rooms = new Table($maxRooms);
        $this->rooms->column('fds', Table::TYPE_STRING, 8192);
        $this->rooms->column('members', Table::TYPE_STRING, $roomMembersBytes);
        $this->rooms->create();
    }

    public function add(int $fd, array $rooms, ?array $user = null): void
    {
        $row = $this->fetch($this->connections, $fd);
        $currentRooms = $row !== null ? $this->decodeStringList($this->str($row, 'rooms')) : [];
        $userId = $row !== null ? $this->str($row, 'userId') : '';
        $userData = $row !== null ? $this->str($row, 'userData') : '';

        $newRooms = array_unique(array_merge($currentRooms, $rooms));

        if ($user !== null) {
            $rawId = $user['id'] ?? '';
            $userId = is_scalar($rawId) ? (string) $rawId : '';
            $userData = json_encode($user, JSON_THROW_ON_ERROR);
        }

        $this->connections->set((string) $fd, [
            'userId' => $userId,
            'userData' => $userData,
            'rooms' => json_encode($newRooms, JSON_THROW_ON_ERROR),
        ]);

        foreach ($rooms as $room) {
            $this->addToRoom($fd, $room, $user);
        }

        if ($userId !== '') {
            $this->addUserFd($userId, $fd);
        }
    }

    public function del(int $fd, array $rooms): void
    {
        foreach ($rooms as $room) {
            $this->removeFromRoom($fd, $room);
        }

        $row = $this->fetch($this->connections, $fd);
        if ($row !== null) {
            $currentRooms = $this->decodeStringList($this->str($row, 'rooms'));
            $remaining = array_values(array_diff($currentRooms, $rooms));
            $this->connections->set((string) $fd, [
                'userId' => $this->str($row, 'userId'),
                'userData' => $this->str($row, 'userData'),
                'rooms' => json_encode($remaining, JSON_THROW_ON_ERROR),
            ]);
        }
    }

    public function delAll(int $fd): void
    {
        $row = $this->fetch($this->connections, $fd);
        if ($row === null) {
            return;
        }

        $rooms = $this->decodeStringList($this->str($row, 'rooms'));
        $userId = $this->str($row, 'userId');

        foreach ($rooms as $room) {
            $this->removeFromRoom($fd, $room);
        }

        if ($userId !== '') {
            $this->removeUserFd($userId, $fd);
        }

        $this->connections->del((string) $fd);
    }

    public function broadcast(string $jsonFrame, array $rooms, array $exceptFds): void
    {
        $exceptSet = array_flip($exceptFds);

        if ($rooms === []) {
            foreach ($this->connections as $fd => $_) {
                $fd = is_numeric($fd) ? (int) $fd : 0;
                if ($fd === 0 || isset($exceptSet[$fd])) {
                    continue;
                }
                $this->sender->pushWs($fd, $jsonFrame);
            }

            return;
        }

        $seen = [];
        foreach ($rooms as $room) {
            $row = $this->fetch($this->rooms, $room);
            if ($row === null) {
                continue;
            }
            $fds = $this->decodeIntList($this->str($row, 'fds'));
            foreach ($fds as $fd) {
                if (isset($exceptSet[$fd]) || isset($seen[$fd])) {
                    continue;
                }
                $seen[$fd] = true;
                $this->sender->pushWs($fd, $jsonFrame);
            }
        }
    }

    public function send(int $fd, string $jsonFrame): void
    {
        $this->sender->pushWs($fd, $jsonFrame);
    }

    public function members(array $rooms): array
    {
        $result = [];
        $seen = [];

        foreach ($rooms as $room) {
            $row = $this->fetch($this->rooms, $room);
            if ($row === null) {
                continue;
            }
            $membersJson = $this->str($row, 'members');
            if ($membersJson === '') {
                continue;
            }
            $members = json_decode($membersJson, true);
            if (!is_array($members)) {
                continue;
            }
            foreach ($members as $fdStr => $userData) {
                $fd = is_numeric($fdStr) ? (int) $fdStr : 0;
                if ($fd === 0 || isset($seen[$fd])) {
                    continue;
                }
                $seen[$fd] = true;
                $result[] = ['fd' => $fd, 'user' => is_array($userData) ? $userData : null];
            }
        }

        return $result;
    }

    public function count(array $rooms): int
    {
        $seen = [];

        foreach ($rooms as $room) {
            $row = $this->fetch($this->rooms, $room);
            if ($row === null) {
                continue;
            }
            $fds = $this->decodeIntList($this->str($row, 'fds'));
            foreach ($fds as $fd) {
                $seen[$fd] = true;
            }
        }

        return count($seen);
    }

    public function userOf(int $fd): ?array
    {
        $row = $this->fetch($this->connections, $fd);
        if ($row === null) {
            return null;
        }
        $userData = $this->str($row, 'userData');
        if ($userData === '') {
            return null;
        }
        $decoded = json_decode($userData, true);

        return is_array($decoded) ? $decoded : null;
    }

    public function fdsOfUser(string $userId): array
    {
        $row = $this->fetch($this->users, $userId);
        if ($row === null) {
            return [];
        }

        return $this->decodeIntList($this->str($row, 'fds'));
    }

    public function roomsOf(int $fd): array
    {
        $row = $this->fetch($this->connections, $fd);
        if ($row === null) {
            return [];
        }

        return $this->decodeStringList($this->str($row, 'rooms'));
    }

    public function disconnectUser(string $userId): void
    {
        foreach ($this->fdsOfUser($userId) as $fd) {
            $this->sender->disconnect($fd, 1000, 'session revoked');
        }
    }

    /**
     * Read a row from a Swoole table by key, or null if absent.
     *
     * @param Table $table
     * @param int|string $key
     *
     * @return array<mixed, mixed>|null
     */
    private function fetch(Table $table, int|string $key): ?array
    {
        $result = $table->get((string) $key);

        if (!is_array($result)) {
            return null;
        }

        return $result;
    }

    /**
     * Safely extract a string column from a Table row.
     *
     * @param array<mixed, mixed> $row
     * @param string $key
     *
     * @return string
     */
    private function str(array $row, string $key): string
    {
        $val = $row[$key] ?? '';

        return is_string($val) ? $val : '';
    }

    /**
     * Record a connection (and its presence identity) as a member of a room.
     *
     * @param int $fd
     * @param string $room
     * @param array<mixed, mixed>|null $user
     *
     * @return void
     */
    private function addToRoom(int $fd, string $room, ?array $user): void
    {
        $row = $this->fetch($this->rooms, $room);
        $fds = $row !== null ? $this->decodeIntList($this->str($row, 'fds')) : [];

        $members = [];
        if ($row !== null) {
            $membersJson = $this->str($row, 'members');
            if ($membersJson !== '') {
                $decoded = json_decode($membersJson, true);
                if (is_array($decoded)) {
                    $members = $decoded;
                }
            }
        }

        if (!in_array($fd, $fds, true)) {
            $fds[] = $fd;
        }

        $members[(string) $fd] = $user;

        $this->rooms->set($room, [
            'fds' => json_encode($fds, JSON_THROW_ON_ERROR),
            'members' => json_encode($members, JSON_THROW_ON_ERROR),
        ]);
    }

    /**
     * Remove from room.
     *
     * @param int $fd
     * @param string $room
     *
     * @return void
     */
    private function removeFromRoom(int $fd, string $room): void
    {
        $row = $this->fetch($this->rooms, $room);
        if ($row === null) {
            return;
        }

        $fds = $this->decodeIntList($this->str($row, 'fds'));
        $fds = array_diff($fds, [$fd]);

        $members = [];
        $membersJson = $this->str($row, 'members');
        if ($membersJson !== '') {
            $decoded = json_decode($membersJson, true);
            if (is_array($decoded)) {
                $members = $decoded;
            }
        }
        unset($members[(string) $fd]);

        if ($fds === []) {
            $this->rooms->del($room);
        } else {
            $this->rooms->set($room, [
                'fds' => json_encode($fds, JSON_THROW_ON_ERROR),
                'members' => json_encode($members, JSON_THROW_ON_ERROR),
            ]);
        }
    }

    /**
     * Add user fd.
     *
     * @param string $userId
     * @param int $fd
     *
     * @return void
     */
    private function addUserFd(string $userId, int $fd): void
    {
        $row = $this->fetch($this->users, $userId);
        $fds = $row !== null ? $this->decodeIntList($this->str($row, 'fds')) : [];

        if (!in_array($fd, $fds, true)) {
            $fds[] = $fd;
        }

        $this->users->set($userId, [
            'fds' => json_encode($fds, JSON_THROW_ON_ERROR),
        ]);
    }

    /**
     * Remove user fd.
     *
     * @param string $userId
     * @param int $fd
     *
     * @return void
     */
    private function removeUserFd(string $userId, int $fd): void
    {
        $row = $this->fetch($this->users, $userId);
        if ($row === null) {
            return;
        }

        $fds = $this->decodeIntList($this->str($row, 'fds'));
        $fds = array_values(array_diff($fds, [$fd]));

        if ($fds === []) {
            $this->users->del($userId);
        } else {
            $this->users->set($userId, [
                'fds' => json_encode($fds, JSON_THROW_ON_ERROR),
            ]);
        }
    }

    /**
     * Decode a JSON array into a list of ints; empty on malformed input.
     *
     * @param string $json
     *
     * @return list<int>
     */
    private function decodeIntList(string $json): array
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
            if (is_int($value) || (is_string($value) && is_numeric($value))) {
                $result[] = (int) $value;
            }
        }

        return $result;
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
