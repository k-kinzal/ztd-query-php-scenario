<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE with ORDER BY and LIMIT on MySQL PDO ZTD.
 *
 * MySQL supports: DELETE FROM t WHERE ... ORDER BY ... LIMIT n
 * This allows deleting only the first N rows matching a condition,
 * sorted by the specified order.
 * @spec pending
 */
class MysqlDeleteWithOrderByLimitTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_dol_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_dol_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_dol_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_dol_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pdo_dol_test (id, name, score) VALUES (3, 'Charlie', 70)");
        $this->pdo->exec("INSERT INTO pdo_dol_test (id, name, score) VALUES (4, 'Dave', 60)");
        $this->pdo->exec("INSERT INTO pdo_dol_test (id, name, score) VALUES (5, 'Eve', 50)");
    }

    /**
     * DELETE with LIMIT only.
     */
    public function testDeleteWithLimit(): void
    {
        $this->pdo->exec('DELETE FROM pdo_dol_test LIMIT 2');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_dol_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE with ORDER BY and LIMIT deletes the N lowest-scoring rows.
     */
    public function testDeleteWithOrderByAndLimit(): void
    {
        $this->pdo->exec('DELETE FROM pdo_dol_test ORDER BY score ASC LIMIT 2');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_dol_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        // The two lowest-scoring (Eve=50, Dave=60) should be deleted
        $stmt = $this->pdo->query('SELECT name FROM pdo_dol_test ORDER BY score ASC');
        $names = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $names[] = $row['name'];
        }
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Charlie', $names);
    }

    /**
     * DELETE with WHERE + ORDER BY + LIMIT.
     */
    public function testDeleteWithWhereOrderByLimit(): void
    {
        $this->pdo->exec('DELETE FROM pdo_dol_test WHERE score < 85 ORDER BY score DESC LIMIT 1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_dol_test');
        $this->assertSame(4, (int) $stmt->fetchColumn());

        // Highest scoring among score < 85 is Bob (80), so Bob should be deleted
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM pdo_dol_test WHERE name = 'Bob'");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation for DELETE with ORDER BY + LIMIT.
     */
    public function testDeleteWithOrderByLimitPhysicalIsolation(): void
    {
        $this->pdo->exec('DELETE FROM pdo_dol_test ORDER BY score ASC LIMIT 3');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_dol_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
