<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Scenarios\WriteOperationScenario;
use Tests\Support\AbstractMysqliTestCase;

class WriteOperationTest extends AbstractMysqliTestCase
{
    use WriteOperationScenario;

    protected function getTableDDL(): string
    {
        return 'CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(255), price INT)';
    }

    protected function getTableNames(): array
    {
        return ['products'];
    }
}
