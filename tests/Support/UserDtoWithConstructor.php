<?php

declare(strict_types=1);

namespace Tests\Support;

class UserDtoWithConstructor
{
    public string $name;
    public int $score;

    public function __construct(
        public string $prefix = '',
    ) {
    }
}
