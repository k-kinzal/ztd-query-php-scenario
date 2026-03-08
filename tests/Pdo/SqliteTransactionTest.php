<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\TransactionScenario;
use Tests\Support\AbstractSqlitePdoTestCase;

class SqliteTransactionTest extends AbstractSqlitePdoTestCase
{
    use TransactionScenario;

    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE tx_test (id INTEGER PRIMARY KEY, val TEXT)';
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
