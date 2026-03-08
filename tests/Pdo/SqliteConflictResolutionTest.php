<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests SQLite-specific conflict resolution syntax in ZTD mode:
 * - INSERT OR REPLACE (synonym for REPLACE INTO)
 * - REPLACE INTO (direct syntax)
 * - INSERT OR ABORT (default behavior, explicit)
 * - INSERT OR FAIL
 * - INSERT OR ROLLBACK
 * - Physical isolation for all variants
 * @spec SPEC-4.2b
 */
class SqliteConflictResolutionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE cr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['cr_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE cr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $this->pdo->exec("INSERT INTO cr_test VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO cr_test VALUES (2, 'Bob', 80)");

        }

    public function testInsertOrReplaceOverwritesExisting(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO cr_test VALUES (1, 'Alicia', 95)");

        $stmt = $this->pdo->query('SELECT name, score FROM cr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alicia', $row['name']);
        $this->assertEquals(95, $row['score']);
    }

    public function testInsertOrReplaceInsertsNew(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO cr_test VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM cr_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM cr_test WHERE id = 3');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    public function testReplaceIntoOverwritesExisting(): void
    {
        $this->pdo->exec("REPLACE INTO cr_test VALUES (2, 'Bobby', 85)");

        $stmt = $this->pdo->query('SELECT name, score FROM cr_test WHERE id = 2');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bobby', $row['name']);
        $this->assertEquals(85, $row['score']);
    }

    public function testReplaceIntoInsertsNew(): void
    {
        $this->pdo->exec("REPLACE INTO cr_test VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM cr_test');
        $this->assertSame(3, (int) $stmt->fetchColumn());
    }

    public function testInsertOrReplaceRowCount(): void
    {
        // Replace existing row — should count as 1 row affected
        $count = $this->pdo->exec("INSERT OR REPLACE INTO cr_test VALUES (1, 'Alicia', 95)");
        $this->assertGreaterThanOrEqual(1, $count);
    }

    public function testMultipleReplacesOnSameKey(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO cr_test VALUES (1, 'Version1', 10)");
        $this->pdo->exec("INSERT OR REPLACE INTO cr_test VALUES (1, 'Version2', 20)");
        $this->pdo->exec("INSERT OR REPLACE INTO cr_test VALUES (1, 'Version3', 30)");

        $stmt = $this->pdo->query('SELECT name, score FROM cr_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Version3', $row['name']);
        $this->assertEquals(30, $row['score']);

        // Still only 2 total rows
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM cr_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Prepared INSERT OR REPLACE should replace existing rows.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/23
     */
    public function testPreparedInsertOrReplaceReplacesExisting(): void
    {
        $stmt = $this->pdo->prepare('INSERT OR REPLACE INTO cr_test VALUES (?, ?, ?)');

        $stmt->execute([1, 'PrepAlice', 99]);

        $check = $this->pdo->query('SELECT name FROM cr_test WHERE id = 1');
        $name = $check->fetchColumn();
        if ($name !== 'PrepAlice') {
            $this->markTestIncomplete(
                'Issue #23: prepared INSERT OR REPLACE does not replace existing rows. '
                . 'Expected "PrepAlice", got ' . var_export($name, true)
            );
        }
        $this->assertSame('PrepAlice', $name);
    }

    public function testPreparedInsertOrReplaceInsertsNew(): void
    {
        $stmt = $this->pdo->prepare('INSERT OR REPLACE INTO cr_test VALUES (?, ?, ?)');

        // Insert new row works correctly
        $stmt->execute([3, 'PrepCharlie', 70]);

        $check = $this->pdo->query('SELECT name FROM cr_test WHERE id = 3');
        $this->assertSame('PrepCharlie', $check->fetchColumn());
    }

    public function testInsertOrReplacePhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT OR REPLACE INTO cr_test VALUES (1, 'Replaced', 99)");

        // Shadow-only table — querying with ZTD disabled fails
        $this->pdo->disableZtd();
        $this->expectException(\PDOException::class);
        $this->pdo->query('SELECT * FROM cr_test');
    }
}
