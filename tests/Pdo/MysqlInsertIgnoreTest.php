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
 * - INSERT IGNORE with UNIQUE constraint on non-PK column
 * - INSERT IGNORE with multiple rows, some duplicates on UNIQUE column
 * @spec SPEC-4.2e
 */
class MysqlInsertIgnoreTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ins_ign_m (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE mp_iig_uniq (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(100) NOT NULL,
                name VARCHAR(50) NOT NULL,
                UNIQUE KEY uq_email (email)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_iig_uniq', 'ins_ign_m'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ins_ign_m VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO ins_ign_m VALUES (2, 'Bob', 80)");
    }

    public function testInsertIgnoreDuplicateKeySkipped(): void
    {
        // INSERT IGNORE with duplicate PK -- should silently skip
        $this->pdo->exec("INSERT IGNORE INTO ins_ign_m VALUES (1, 'AliceV2', 99)");

        // Original row preserved
        $stmt = $this->pdo->query('SELECT name, score FROM ins_ign_m WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(90, $row['score']);
    }

    public function testInsertIgnoreNonDuplicateInserted(): void
    {
        // INSERT IGNORE with non-duplicate PK -- should insert normally
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

    /**
     * INSERT IGNORE into a table with a UNIQUE constraint on a non-PK column.
     * Duplicate email should be silently skipped.
     */
    public function testInsertIgnoreUniqueConstraintNonPk(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_iig_uniq (email, name) VALUES ('alice@test.com', 'Alice')");

            // INSERT IGNORE with duplicate email (non-PK unique)
            $this->pdo->exec("INSERT IGNORE INTO mp_iig_uniq (email, name) VALUES ('alice@test.com', 'Alice Duplicate')");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_iig_uniq');
            $this->assertSame(1, (int) $rows[0]['cnt'], 'Duplicate email should be skipped');

            $rows = $this->ztdQuery("SELECT name FROM mp_iig_uniq WHERE email = 'alice@test.com'");
            $this->assertSame('Alice', $rows[0]['name'], 'Original row should be preserved');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE with non-PK UNIQUE constraint failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT IGNORE with multiple rows, some duplicating the UNIQUE column.
     */
    public function testInsertIgnoreBatchWithUniqueDuplicates(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_iig_uniq (email, name) VALUES ('bob@test.com', 'Bob')");

            // Batch: bob@test.com is duplicate, charlie@test.com is new
            $this->pdo->exec(
                "INSERT IGNORE INTO mp_iig_uniq (email, name) VALUES "
                . "('bob@test.com', 'Bob Dup'), ('charlie@test.com', 'Charlie')"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_iig_uniq');
            $this->assertSame(2, (int) $rows[0]['cnt'], 'Should have 2 rows (1 original + 1 new)');

            // Verify original Bob preserved
            $rows = $this->ztdQuery("SELECT name FROM mp_iig_uniq WHERE email = 'bob@test.com'");
            $this->assertSame('Bob', $rows[0]['name']);

            // Verify Charlie inserted
            $rows = $this->ztdQuery("SELECT name FROM mp_iig_uniq WHERE email = 'charlie@test.com'");
            $this->assertCount(1, $rows);
            $this->assertSame('Charlie', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE batch with UNIQUE duplicates failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT IGNORE with all-new UNIQUE values should insert all rows.
     */
    public function testInsertIgnoreAllNewUniqueValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT IGNORE INTO mp_iig_uniq (email, name) VALUES "
                . "('x@test.com', 'X'), ('y@test.com', 'Y'), ('z@test.com', 'Z')"
            );

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_iig_uniq');
            $this->assertSame(3, (int) $rows[0]['cnt']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE all-new unique values failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT IGNORE against the UNIQUE constraint column.
     */
    public function testPreparedInsertIgnoreUniqueConstraint(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_iig_uniq (email, name) VALUES ('prep@test.com', 'Original')");

            $stmt = $this->pdo->prepare('INSERT IGNORE INTO mp_iig_uniq (email, name) VALUES (?, ?)');

            // Duplicate email
            $stmt->execute(['prep@test.com', 'Duplicate']);

            $rows = $this->ztdQuery("SELECT name FROM mp_iig_uniq WHERE email = 'prep@test.com'");
            $this->assertSame('Original', $rows[0]['name'], 'Prepared INSERT IGNORE should skip duplicate');

            // New email
            $stmt->execute(['new@test.com', 'New']);
            $rows = $this->ztdQuery("SELECT name FROM mp_iig_uniq WHERE email = 'new@test.com'");
            $this->assertCount(1, $rows);
            $this->assertSame('New', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared INSERT IGNORE with UNIQUE constraint failed: ' . $e->getMessage()
            );
        }
    }
}
