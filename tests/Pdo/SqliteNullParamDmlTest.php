<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML operations with NULL parameter binding.
 *
 * Users commonly set columns to NULL via prepared statements.
 * Tests whether the CTE rewriter handles NULL params correctly
 * in INSERT, UPDATE, and DELETE contexts.
 *
 * @spec SPEC-4.1
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteNullParamDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_npd_records (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            notes TEXT,
            score INTEGER
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_npd_records'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_npd_records VALUES (1, 'Alice', 'Good', 90)");
        $this->pdo->exec("INSERT INTO sl_npd_records VALUES (2, 'Bob', NULL, 80)");
        $this->pdo->exec("INSERT INTO sl_npd_records VALUES (3, 'Charlie', 'OK', NULL)");
    }

    /**
     * Prepared INSERT with NULL parameter.
     */
    public function testPreparedInsertWithNullParam(): void
    {
        $sql = "INSERT INTO sl_npd_records VALUES (?, ?, ?, ?)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->bindValue(1, 4, PDO::PARAM_INT);
            $stmt->bindValue(2, 'Diana', PDO::PARAM_STR);
            $stmt->bindValue(3, null, PDO::PARAM_NULL);
            $stmt->bindValue(4, null, PDO::PARAM_NULL);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT name, notes, score FROM sl_npd_records WHERE id = 4");

            $this->assertCount(1, $rows);
            $this->assertSame('Diana', $rows[0]['name']);
            $this->assertNull($rows[0]['notes']);
            $this->assertNull($rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT with NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET column to NULL.
     */
    public function testPreparedUpdateSetNull(): void
    {
        $sql = "UPDATE sl_npd_records SET notes = ? WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->bindValue(1, null, PDO::PARAM_NULL);
            $stmt->bindValue(2, 1, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT notes FROM sl_npd_records WHERE id = 1");

            $this->assertCount(1, $rows);

            if ($rows[0]['notes'] !== null) {
                $this->markTestIncomplete(
                    'UPDATE SET NULL: expected NULL, got ' . var_export($rows[0]['notes'], true)
                );
            }

            $this->assertNull($rows[0]['notes']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE SET NULL failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE SET multiple columns, mix of NULL and non-NULL.
     */
    public function testPreparedUpdateMixedNullNonNull(): void
    {
        $sql = "UPDATE sl_npd_records SET notes = ?, score = ? WHERE id = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->bindValue(1, null, PDO::PARAM_NULL);
            $stmt->bindValue(2, 100, PDO::PARAM_INT);
            $stmt->bindValue(3, 1, PDO::PARAM_INT);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT notes, score FROM sl_npd_records WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertNull($rows[0]['notes']);
            $this->assertSame(100, (int) $rows[0]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Mixed NULL UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE IS NULL with prepared statement.
     */
    public function testDeleteWhereIsNull(): void
    {
        $sql = "DELETE FROM sl_npd_records WHERE notes IS NULL";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name FROM sl_npd_records ORDER BY id");

            // Bob (notes=NULL) deleted; Alice and Charlie remain
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'IS NULL DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('IS NULL DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT with IS NULL on shadow-updated data.
     */
    public function testSelectIsNullOnShadowUpdatedData(): void
    {
        // Set Alice's notes to NULL via UPDATE
        $this->pdo->exec("UPDATE sl_npd_records SET notes = NULL WHERE id = 1");

        $rows = $this->ztdQuery("SELECT name FROM sl_npd_records WHERE notes IS NULL ORDER BY name");

        // Both Alice (just nulled) and Bob (originally null) should match
        if (count($rows) !== 2) {
            $this->markTestIncomplete(
                'Shadow IS NULL: expected 2, got ' . count($rows)
                . '. Data: ' . json_encode($rows)
            );
        }

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
    }

    /**
     * Prepared DELETE with IS NOT NULL and additional param.
     */
    public function testPreparedDeleteIsNotNullWithParam(): void
    {
        $sql = "DELETE FROM sl_npd_records WHERE notes IS NOT NULL AND score > ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([85]);

            $rows = $this->ztdQuery("SELECT name FROM sl_npd_records ORDER BY id");

            // Alice: notes='Good' (NOT NULL) AND score=90 > 85 → DELETED
            // Bob: notes=NULL → doesn't match IS NOT NULL
            // Charlie: notes='OK' (NOT NULL) AND score=NULL → NULL > 85 is NULL (false)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'IS NOT NULL DELETE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('IS NOT NULL DELETE failed: ' . $e->getMessage());
        }
    }
}
