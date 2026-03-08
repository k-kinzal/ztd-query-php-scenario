<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT IGNORE behavior on MySQL ZTD:
 * - Duplicate PK silently skipped
 * - Non-duplicate rows inserted
 * - Batch INSERT IGNORE with mixed duplicate/non-duplicate
 * - Prepared INSERT IGNORE
 * @spec SPEC-4.2e
 */
class MysqlInsertIgnoreTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ins_ign_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['ins_ign_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ins_ign_m VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO ins_ign_m VALUES (2, 'Bob', 80)");
    }

    public function testInsertIgnoreDuplicateKeySkipped(): void
    {
        // INSERT IGNORE with duplicate PK — should silently skip
        $this->pdo->exec("INSERT IGNORE INTO ins_ign_m VALUES (1, 'AliceV2', 99)");

        // Original row preserved
        $stmt = $this->pdo->query('SELECT name, score FROM ins_ign_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(90, $row['score']);
    }

    public function testInsertIgnoreNonDuplicateInserted(): void
    {
        // INSERT IGNORE with non-duplicate PK — should insert normally
        $this->pdo->exec("INSERT IGNORE INTO ins_ign_m VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ins_ign_m');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM ins_ign_m WHERE id = 3');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    public function testInsertIgnoreBatchMixedDuplicates(): void
    {
        // Batch INSERT IGNORE: id=1 is duplicate, id=3 is new
        $this->pdo->exec("INSERT IGNORE INTO ins_ign_m VALUES (1, 'DuplicateAlice', 99), (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ins_ign_m');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        // Duplicate row unchanged
        $stmt = $this->pdo->query('SELECT name FROM ins_ign_m WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());

        // New row inserted
        $stmt = $this->pdo->query('SELECT name FROM ins_ign_m WHERE id = 3');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    public function testInsertIgnoreAllDuplicates(): void
    {
        // INSERT IGNORE where ALL rows are duplicates
        $this->pdo->exec("INSERT IGNORE INTO ins_ign_m VALUES (1, 'DupAlice', 99), (2, 'DupBob', 99)");

        // Count unchanged
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ins_ign_m');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testPreparedInsertIgnore(): void
    {
        $stmt = $this->pdo->prepare('INSERT IGNORE INTO ins_ign_m VALUES (?, ?, ?)');

        // Execute with duplicate PK
        $stmt->execute([1, 'DupAlice', 99]);

        // Original preserved
        $check = $this->pdo->query('SELECT name FROM ins_ign_m WHERE id = 1');
        $this->assertSame('Alice', $check->fetchColumn());

        // Execute with non-duplicate PK
        $stmt->execute([3, 'Charlie', 70]);

        $check = $this->pdo->query('SELECT name FROM ins_ign_m WHERE id = 3');
        $this->assertSame('Charlie', $check->fetchColumn());
    }

    public function testInsertIgnoreDoesNotAffectSubsequentInserts(): void
    {
        // INSERT IGNORE silently skips duplicate
        $this->pdo->exec("INSERT IGNORE INTO ins_ign_m VALUES (1, 'DupAlice', 99)");

        // Normal INSERT with a new PK after INSERT IGNORE still works
        $this->pdo->exec("INSERT INTO ins_ign_m VALUES (4, 'Diana', 85)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ins_ign_m');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }
}
