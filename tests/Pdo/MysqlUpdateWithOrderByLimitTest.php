<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE with ORDER BY and LIMIT on MySQL PDO ZTD.
 *
 * MySQL supports: UPDATE t SET ... WHERE ... ORDER BY ... LIMIT n
 * This allows updating only the first N rows matching a condition,
 * sorted by the specified order.
 * @spec pending
 */
class MysqlUpdateWithOrderByLimitTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_uol_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, status VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_uol_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_uol_test (id, name, score, status) VALUES (1, 'Alice', 90, 'active')");
        $this->pdo->exec("INSERT INTO pdo_uol_test (id, name, score, status) VALUES (2, 'Bob', 80, 'active')");
        $this->pdo->exec("INSERT INTO pdo_uol_test (id, name, score, status) VALUES (3, 'Charlie', 70, 'active')");
        $this->pdo->exec("INSERT INTO pdo_uol_test (id, name, score, status) VALUES (4, 'Dave', 60, 'active')");
        $this->pdo->exec("INSERT INTO pdo_uol_test (id, name, score, status) VALUES (5, 'Eve', 50, 'active')");
    }

    /**
     * UPDATE with LIMIT only.
     */
    public function testUpdateWithLimit(): void
    {
        $this->pdo->exec("UPDATE pdo_uol_test SET status = 'updated' LIMIT 2");

        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_uol_test WHERE status = 'updated'");
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE with ORDER BY and LIMIT updates only the first N matching rows.
     */
    public function testUpdateWithOrderByAndLimit(): void
    {
        $this->pdo->exec("UPDATE pdo_uol_test SET status = 'low' ORDER BY score ASC LIMIT 2");

        // Two lowest-scoring should be marked 'low'
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_uol_test WHERE status = 'low'");
        $this->assertSame(2, (int) $stmt->fetchColumn());

        // Eve (50) and Dave (60) should be updated
        $stmt = $this->pdo->query("SELECT status FROM pdo_uol_test WHERE id = 5");
        $this->assertSame('low', $stmt->fetch(PDO::FETCH_ASSOC)['status']);

        $stmt = $this->pdo->query("SELECT status FROM pdo_uol_test WHERE id = 4");
        $this->assertSame('low', $stmt->fetch(PDO::FETCH_ASSOC)['status']);

        // Alice should remain active
        $stmt = $this->pdo->query("SELECT status FROM pdo_uol_test WHERE id = 1");
        $this->assertSame('active', $stmt->fetch(PDO::FETCH_ASSOC)['status']);
    }

    /**
     * UPDATE with WHERE + ORDER BY + LIMIT.
     */
    public function testUpdateWithWhereOrderByLimit(): void
    {
        $this->pdo->exec("UPDATE pdo_uol_test SET status = 'top' WHERE score > 60 ORDER BY score DESC LIMIT 2");

        // Top 2 of those with score > 60: Alice (90) and Bob (80)
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_uol_test WHERE status = 'top'");
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query("SELECT status FROM pdo_uol_test WHERE id = 1");
        $this->assertSame('top', $stmt->fetch(PDO::FETCH_ASSOC)['status']);

        $stmt = $this->pdo->query("SELECT status FROM pdo_uol_test WHERE id = 2");
        $this->assertSame('top', $stmt->fetch(PDO::FETCH_ASSOC)['status']);
    }

    /**
     * Physical isolation for UPDATE with ORDER BY + LIMIT.
     */
    public function testUpdateWithOrderByLimitPhysicalIsolation(): void
    {
        $this->pdo->exec("UPDATE pdo_uol_test SET status = 'changed' ORDER BY score ASC LIMIT 3");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_uol_test WHERE status = 'changed'");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
