<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests that query rewriting occurs at prepare time, not execute time (MySQL PDO).
 * @spec SPEC-2.1
 */
class MysqlPrepareTimeRewritingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_ptr_items (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_ptr_items'];
    }


    public function testSelectPreparedWithZtdEnabledDisabledBeforeExecute(): void
    {
        $this->pdo->exec("INSERT INTO mysql_ptr_items VALUES (10, 'Shadow X', 99.99)");

        $stmt = $this->pdo->prepare('SELECT * FROM mysql_ptr_items WHERE id = ?');

        $this->pdo->disableZtd();

        $stmt->execute([10]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Shadow X', $rows[0]['name']);

        $this->pdo->enableZtd();
    }

    public function testSelectPreparedWithZtdDisabledEnabledBeforeExecute(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->prepare('SELECT * FROM mysql_ptr_items ORDER BY id');

        $this->pdo->enableZtd();

        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Physical A', $rows[0]['name']);
        $this->assertSame('Physical B', $rows[1]['name']);
    }

    public function testTwoPreparedStatementsOppositeToggle(): void
    {
        $this->pdo->exec("INSERT INTO mysql_ptr_items VALUES (10, 'Shadow Only', 50.00)");

        $stmtShadow = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM mysql_ptr_items');

        $this->pdo->disableZtd();
        $stmtPhysical = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM mysql_ptr_items');

        $this->pdo->enableZtd();

        $stmtShadow->execute();
        $shadowCount = (int) $stmtShadow->fetch(PDO::FETCH_ASSOC)['cnt'];

        $stmtPhysical->execute();
        $physicalCount = (int) $stmtPhysical->fetch(PDO::FETCH_ASSOC)['cnt'];

        $this->assertSame(1, $shadowCount);
        $this->assertSame(2, $physicalCount);
    }

    public function testReExecuteAcrossMultipleToggles(): void
    {
        $this->pdo->exec("INSERT INTO mysql_ptr_items VALUES (1, 'Shadow A', 10.00)");
        $this->pdo->exec("INSERT INTO mysql_ptr_items VALUES (2, 'Shadow B', 20.00)");

        $stmt = $this->pdo->prepare('SELECT COUNT(*) AS cnt FROM mysql_ptr_items');

        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        $this->pdo->disableZtd();
        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);

        $this->pdo->enableZtd();
        $stmt->execute();
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }
}
