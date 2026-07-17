<?php

declare(strict_types=1);

namespace PHPdot\Realtime\Tests\Unit;

use PHPdot\Realtime\Event;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

/**
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */
final class EventTest extends TestCase
{
    #[Test]
    public function encodesEventAndPayload(): void
    {
        self::assertSame('{"event":"chat.message","data":{"text":"hi"}}', Event::encode('chat.message', ['text' => 'hi']));
    }

    #[Test]
    public function encodesNullPayload(): void
    {
        self::assertSame('{"event":"ping","data":null}', Event::encode('ping'));
    }

    #[Test]
    public function decodesEventChannelDataAndAck(): void
    {
        $decoded = Event::decode('{"event":"message","channel":"chat:public","data":{"x":1},"ack":7}');

        self::assertNotNull($decoded);
        self::assertSame('message', $decoded['event']);
        self::assertSame('chat:public', $decoded['channel']);
        self::assertSame(['x' => 1], $decoded['data']);
        self::assertSame(7, $decoded['ack']);
    }

    #[Test]
    public function defaultsChannelAndAckToNull(): void
    {
        $decoded = Event::decode('{"event":"message","data":"hi"}');

        self::assertNotNull($decoded);
        self::assertNull($decoded['channel']);
        self::assertNull($decoded['ack']);
        self::assertSame('hi', $decoded['data']);
    }

    #[Test]
    public function returnsNullForMalformedFrames(): void
    {
        self::assertNull(Event::decode('not json'));
        self::assertNull(Event::decode('{"no":"event"}'));
        self::assertNull(Event::decode('{"event":123}'));
        self::assertNull(Event::decode('[]'));
    }
}
