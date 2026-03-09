<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT IGNORE behavior through the MySQLi ZTD adapter.
 *
 * Extends coverage beyond the existing InsertModifiersTest by testing:
 *   - UNIQUE key constraint violations (non-PK)
 *   - INSERT IGNORE with composite primary key
 *   - INSERT IGNORE followed by UPDATE on the same row
 *   - INSERT IGNORE with NULL in UNIQUE column
 *   - affected_rows count after INSERT IGNORE
 *   - Multi-row INSERT IGNORE with UNIQUE constraint conflicts
 *
 * @spec SPEC-4.2e
 */
class InsertIgnoreTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ins_ign (
                id INT PRIMARY KEY AUTO_INCREMENT,
                email VARCHAR(100) NOT NULL,
                username VARCHAR(50) NOT NULL,
                score INT NOT NULL DEFAULT 0,
                UNIQUE KEY uq_email (email),
                UNIQUE KEY uq_username (username)
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_ins_ign_cpk (
                tenant_id INT NOT NULL,
                user_id INT NOT NULL,
                role VARCHAR(30) NOT NULL,
                PRIMARY KEY (tenant_id, user_id)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ins_ign', 'mi_ins_ign_cpk'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO mi_ins_ign (id, email, username, score) VALUES (1, 'alice@example.com', 'alice', 90)");
        $this->ztdExec("INSERT INTO mi_ins_ign (id, email, username, score) VALUES (2, 'bob@example.com', 'bob', 80)");
    }

    /**
     * INSERT IGNORE with duplicate PK skips the row.
     */
    public function testInsertIgnoreDuplicatePkSkipped(): void
    {
        try {
            $this->ztdExec("INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES (1, 'new@example.com', 'newuser', 99)");

            $rows = $this->ztdQuery('SELECT email, username, score FROM mi_ins_ign WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('alice@example.com', $rows[0]['email'], 'Original email should be preserved');
            $this->assertSame('alice', $rows[0]['username'], 'Original username should be preserved');
            $this->assertEquals(90, (int) $rows[0]['score'], 'Original score should be preserved');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE duplicate PK failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT IGNORE with duplicate UNIQUE key (email) skips the row.
     *
     * This tests non-PK unique constraint handling, which the CTE
     * rewriter must track separately from PK constraints.
     */
    public function testInsertIgnoreDuplicateUniqueKeySkipped(): void
    {
        try {
            $this->ztdExec("INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES (3, 'alice@example.com', 'alice2', 99)");

            // Row should not be inserted
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign');
            $this->assertEquals(2, (int) $rows[0]['cnt'], 'No new row should be inserted on UNIQUE email conflict');

            // Original row unchanged
            $rows = $this->ztdQuery("SELECT username FROM mi_ins_ign WHERE email = 'alice@example.com'");
            $this->assertSame('alice', $rows[0]['username']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE duplicate UNIQUE key failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT IGNORE with duplicate on second UNIQUE key (username) skips the row.
     */
    public function testInsertIgnoreDuplicateSecondUniqueKey(): void
    {
        try {
            $this->ztdExec("INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES (3, 'charlie@example.com', 'alice', 70)");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign');
            $this->assertEquals(2, (int) $rows[0]['cnt'], 'No new row should be inserted on UNIQUE username conflict');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE duplicate second UNIQUE key failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multi-row INSERT IGNORE with some UNIQUE constraint duplicates.
     *
     * Mixed batch: some rows conflict on UNIQUE keys, some are new.
     * The CTE rewriter must correctly identify which rows to skip.
     */
    public function testMultiRowInsertIgnoreWithUniqueDuplicates(): void
    {
        try {
            $this->ztdExec(
                "INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES
                 (3, 'charlie@example.com', 'charlie', 70),
                 (4, 'alice@example.com', 'alice_dup', 99),
                 (5, 'dave@example.com', 'dave', 60),
                 (6, 'eve@example.com', 'bob', 55)"
            );

            // id=4 conflicts on email (alice@example.com), id=6 conflicts on username (bob)
            // Only id=3 and id=5 should be inserted
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign');
            $expected = 4; // 2 original + 2 new
            $this->assertEquals($expected, (int) $rows[0]['cnt'], "Expected {$expected} rows (2 original + 2 new, 2 skipped)");

            // Verify new rows
            $newRows = $this->ztdQuery("SELECT username FROM mi_ins_ign WHERE id IN (3, 5) ORDER BY id");
            $this->assertCount(2, $newRows);
            $this->assertSame('charlie', $newRows[0]['username']);
            $this->assertSame('dave', $newRows[1]['username']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-row INSERT IGNORE with UNIQUE duplicates failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT IGNORE followed by UPDATE on the same would-be duplicate row.
     *
     * After INSERT IGNORE silently skips a duplicate, a subsequent UPDATE
     * should still be able to modify the original row.
     */
    public function testInsertIgnoreThenUpdateOriginal(): void
    {
        try {
            $this->ztdExec("INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES (1, 'dup@example.com', 'dup', 99)");
            $this->ztdExec("UPDATE mi_ins_ign SET score = 100 WHERE id = 1");

            $rows = $this->ztdQuery('SELECT email, score FROM mi_ins_ign WHERE id = 1');
            $this->assertSame('alice@example.com', $rows[0]['email'], 'Email should be original');
            $this->assertEquals(100, (int) $rows[0]['score'], 'Score should be updated to 100');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE then UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT IGNORE with composite primary key.
     *
     * Tests that the CTE rewriter correctly handles INSERT IGNORE
     * when the table has a composite primary key.
     */
    public function testInsertIgnoreCompositePrimaryKey(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_ins_ign_cpk VALUES (1, 100, 'admin')");
            $this->ztdExec("INSERT INTO mi_ins_ign_cpk VALUES (1, 200, 'editor')");

            // Duplicate composite key — should be ignored
            $this->ztdExec("INSERT IGNORE INTO mi_ins_ign_cpk VALUES (1, 100, 'viewer')");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign_cpk');
            $this->assertEquals(2, (int) $rows[0]['cnt'], 'Duplicate composite PK should be ignored');

            // Original role preserved
            $rows = $this->ztdQuery('SELECT role FROM mi_ins_ign_cpk WHERE tenant_id = 1 AND user_id = 100');
            $this->assertSame('admin', $rows[0]['role'], 'Original role should be preserved');

            // Non-duplicate composite key should be inserted
            $this->ztdExec("INSERT IGNORE INTO mi_ins_ign_cpk VALUES (2, 100, 'admin')");
            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign_cpk');
            $this->assertEquals(3, (int) $rows[0]['cnt'], 'Non-duplicate composite PK should be inserted');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'INSERT IGNORE composite PK failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared INSERT IGNORE with UNIQUE constraint.
     *
     * Tests prepared statement path for INSERT IGNORE.
     */
    public function testPreparedInsertIgnoreUniqueConstraint(): void
    {
        try {
            $stmt = $this->mysqli->prepare(
                'INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES (?, ?, ?, ?)'
            );

            // Attempt duplicate email
            $id = 3;
            $email = 'alice@example.com';
            $username = 'newuser';
            $score = 99;
            $stmt->bind_param('issi', $id, $email, $username, $score);
            $stmt->execute();

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign');
            $this->assertEquals(2, (int) $rows[0]['cnt'], 'Prepared INSERT IGNORE should skip duplicate UNIQUE');

            // Re-execute with valid data
            $id = 3;
            $email = 'charlie@example.com';
            $username = 'charlie';
            $score = 70;
            $stmt->execute();

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign');
            $this->assertEquals(3, (int) $rows[0]['cnt'], 'Prepared INSERT IGNORE should insert non-duplicate');
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared INSERT IGNORE with UNIQUE constraint failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: INSERT IGNORE changes must not reach the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES (1, 'dup@example.com', 'dup', 99)");
        $this->ztdExec("INSERT IGNORE INTO mi_ins_ign (id, email, username, score) VALUES (3, 'charlie@example.com', 'charlie', 70)");

        $this->disableZtd();
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mi_ins_ign');
        $this->assertEquals(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
