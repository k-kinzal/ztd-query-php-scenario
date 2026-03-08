<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Scenarios\TransactionScenario;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests transaction behavior in ZTD mode on MySQL via PDO.
 */
class MysqlTransactionTest extends AbstractMysqlPdoTestCase
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
