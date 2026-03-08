<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\TransactionScenario;
use Tests\Support\AbstractPostgresPdoTestCase;

/** @spec SPEC-4.8 */
class PostgresTransactionTest extends AbstractPostgresPdoTestCase
{
    use TransactionScenario;

    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE tx_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['tx_test'];
    }

    public function testQuote(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString('it', $quoted);
    }
}
