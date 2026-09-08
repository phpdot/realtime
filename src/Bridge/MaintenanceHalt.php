<?php

declare(strict_types=1);

/**
 * Cancels the membership maintenance's ticks when a worker exits — the other
 * half of {@see MaintenanceTick}.
 *
 * The ticks hold the worker's event loop open, so an uncanceled maintenance
 * round turns every stop and reload into a full drain-window wait. Resolves
 * the SAME MaintenanceTick instance the WorkerStarted listener armed (it is a
 * singleton), so the ids it cancels are the ids that were set.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Realtime\Bridge;

use PHPdot\Server\Attribute\ServerListener;
use PHPdot\Server\Event\WorkerExiting;

#[ServerListener]
final class MaintenanceHalt
{
    public function __construct(private readonly MaintenanceTick $tick) {}

    /**
     * @param WorkerExiting $event The lifecycle event
     *
     * @return void
     */
    public function __invoke(WorkerExiting $event): void
    {
        $this->tick->halt();
    }
}
