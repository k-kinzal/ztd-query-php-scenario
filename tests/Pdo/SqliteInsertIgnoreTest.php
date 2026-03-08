<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT OR IGNORE behavior on SQLite ZTD:
 * - Duplicate PK silently skipped
 * - Non-duplicate rows inserted
 * - Prepared INSERT OR IGNORE
 * - Physical isolation (shadow-only table)
 *
 * Note: SQLite uses "INSERT OR IGNORE" syntax (not "INSERT IGNORE").
 * @spec SPEC-4.2e
 */
class SqliteInsertIgnoreTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE ins_ign_s (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['ins_ign_s'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ins_ign_s VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO ins_ign_s VALUES (2, 'Bob', 80)");

        }

    public function testInsertOrIgnoreDuplicateSkipped(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO ins_ign_s VALUES (1, 'AliceV2', 99)");

        $stmt = $this->pdo->query('SELECT name, score FROM ins_ign_s WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(90, $row['score']);
    }

    public function testInsertOrIgnoreNonDuplicateInserted(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO ins_ign_s VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ins_ign_s');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM ins_ign_s WHERE id = 3');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    public function testInsertOrIgnoreAllDuplicates(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO ins_ign_s VALUES (1, 'DupAlice', 99)");
        $this->pdo->exec("INSERT OR IGNORE INTO ins_ign_s VALUES (2, 'DupBob', 99)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ins_ign_s');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    public function testPreparedInsertOrIgnore(): void
    {
        $stmt = $this->pdo->prepare('INSERT OR IGNORE INTO ins_ign_s VALUES (?, ?, ?)');

        // Duplicate PK
        $stmt->execute([1, 'DupAlice', 99]);

        $check = $this->pdo->query('SELECT name FROM ins_ign_s WHERE id = 1');
        $this->assertSame('Alice', $check->fetchColumn());

        // Non-duplicate PK
        $stmt->execute([3, 'Charlie', 70]);

        $check = $this->pdo->query('SELECT name FROM ins_ign_s WHERE id = 3');
        $this->assertSame('Charlie', $check->fetchColumn());
    }

    public function testInsertOrIgnoreDoesNotAffectSubsequentInserts(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO ins_ign_s VALUES (1, 'DupAlice', 99)");

        // Normal INSERT with new PK after INSERT OR IGNORE still works
        $this->pdo->exec("INSERT INTO ins_ign_s VALUES (4, 'Diana', 85)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ins_ign_s');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testInsertOrIgnorePhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT OR IGNORE INTO ins_ign_s VALUES (1, 'DupAlice', 99)");
        $this->pdo->exec("INSERT OR IGNORE INTO ins_ign_s VALUES (3, 'Charlie', 70)");

        // Shadow-only table — querying with ZTD disabled fails on SQLite
        $this->pdo->disableZtd();
        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT COUNT(*) FROM ins_ign_s');
    }
}
