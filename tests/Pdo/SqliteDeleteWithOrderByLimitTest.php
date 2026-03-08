<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with ORDER BY and LIMIT on SQLite PDO ZTD.
 *
 * SQLite supports: DELETE FROM t ORDER BY col LIMIT n
 * This allows deleting only the first N rows in a specified order.
 * @spec SPEC-4.3
 */
class SqliteDeleteWithOrderByLimitTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_dol_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sl_dol_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dol_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO sl_dol_test (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO sl_dol_test (id, name, score) VALUES (3, 'Charlie', 70)");
        $this->pdo->exec("INSERT INTO sl_dol_test (id, name, score) VALUES (4, 'Dave', 60)");
        $this->pdo->exec("INSERT INTO sl_dol_test (id, name, score) VALUES (5, 'Eve', 50)");
    }

    /**
     * DELETE with ORDER BY and LIMIT deletes the N lowest-scoring rows.
     */
    public function testDeleteWithOrderByAndLimit(): void
    {
        $this->pdo->exec('DELETE FROM sl_dol_test ORDER BY score ASC LIMIT 2');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dol_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        // The two lowest-scoring (Eve=50, Dave=60) should be deleted
        $stmt = $this->pdo->query('SELECT name FROM sl_dol_test ORDER BY score ASC');
        $names = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Charlie', 'Bob', 'Alice'], $names);
    }

    /**
     * DELETE oldest N rows by id (delete the first 3 inserted).
     */
    public function testDeleteOldestNRows(): void
    {
        $this->pdo->exec('DELETE FROM sl_dol_test ORDER BY id ASC LIMIT 3');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dol_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        // ids 1,2,3 deleted — Dave and Eve remain
        $stmt = $this->pdo->query('SELECT name FROM sl_dol_test ORDER BY id ASC');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Dave', 'Eve'], $names);
    }

    /**
     * DELETE with WHERE + ORDER BY + LIMIT.
     */
    public function testDeleteWithWhereOrderByLimit(): void
    {
        $this->pdo->exec('DELETE FROM sl_dol_test WHERE score < 85 ORDER BY score DESC LIMIT 1');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dol_test');
        $this->assertSame(4, (int) $stmt->fetchColumn());

        // Highest scoring among score < 85 is Bob (80), so Bob should be deleted
        $stmt = $this->pdo->query("SELECT COUNT(*) FROM sl_dol_test WHERE name = 'Bob'");
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation: ZTD deletions are not visible after disableZtd().
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('DELETE FROM sl_dol_test ORDER BY score ASC LIMIT 3');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dol_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Prepared DELETE with ORDER BY and LIMIT using a bound parameter.
     */
    public function testPreparedDeleteWithOrderByLimit(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM sl_dol_test ORDER BY score ASC LIMIT ?');
        $stmt->execute([2]);

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_dol_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        // Verify the two lowest (Eve=50, Dave=60) are gone
        $stmt = $this->pdo->query('SELECT name FROM sl_dol_test ORDER BY score ASC');
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Charlie', 'Bob', 'Alice'], $names);
    }
}
