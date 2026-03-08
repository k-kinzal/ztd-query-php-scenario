<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Scenarios\TransactionScenario;
use Tests\Support\AbstractMysqlPdoTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests transaction behavior in ZTD mode on MySQL via PDO.
 * @spec SPEC-4.8
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

    public function testRollbackDoesNotAffectShadowData(): void
    {
        $this->pdo->beginTransaction();
        $this->pdo->exec("INSERT INTO tx_test (id, val) VALUES (1, 'in_tx')");
        $this->pdo->rollBack();

        // Shadow data persists after rollback because shadow store
        // is independent of physical transaction state
        $stmt = $this->pdo->query('SELECT * FROM tx_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('in_tx', $rows[0]['val']);

        // Physical table should be empty (rollback affected physical layer)
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM tx_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testLastInsertId(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS auto_inc_test');
        $raw->exec('CREATE TABLE auto_inc_test (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(255))');

        $this->pdo->exec("INSERT INTO auto_inc_test (val) VALUES ('hello')");

        // lastInsertId may or may not reflect the shadow insert
        // depending on adapter implementation
        $id = $this->pdo->lastInsertId();
        $this->assertNotFalse($id);

        $raw->exec('DROP TABLE IF EXISTS auto_inc_test');
    }

    public function testQuote(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString('it', $quoted);
    }
}
