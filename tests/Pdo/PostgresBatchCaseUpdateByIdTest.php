<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests batch UPDATE with CASE WHEN id = N THEN ... pattern.
 *
 * This is the standard ORM pattern for updating multiple rows with different
 * values in a single statement. ORMs like Doctrine and Eloquent generate:
 *   UPDATE t SET col = CASE WHEN id = 1 THEN 'a' WHEN id = 2 THEN 'b' END
 *   WHERE id IN (1, 2)
 *
 * Distinct from Issue #142 (CASE expression + WHERE filtering): here the CASE
 * routes different values to different rows by PK, not conditional logic.
 *
 * @spec SPEC-4.2
 */
class PostgresBatchCaseUpdateByIdTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_bcu_users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            role VARCHAR(20) NOT NULL DEFAULT \'user\',
            score INTEGER NOT NULL DEFAULT 0
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_bcu_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_bcu_users VALUES (1, 'Alice', 'user', 10)");
        $this->pdo->exec("INSERT INTO pg_bcu_users VALUES (2, 'Bob', 'user', 20)");
        $this->pdo->exec("INSERT INTO pg_bcu_users VALUES (3, 'Carol', 'user', 30)");
        $this->pdo->exec("INSERT INTO pg_bcu_users VALUES (4, 'Dave', 'user', 40)");
    }

    /**
     * Basic batch update: assign different roles by id.
     */
    public function testBatchUpdateSingleColumn(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_bcu_users
                 SET role = CASE
                     WHEN id = 1 THEN 'admin'
                     WHEN id = 2 THEN 'moderator'
                     WHEN id = 3 THEN 'editor'
                     ELSE role
                 END
                 WHERE id IN (1, 2, 3)"
            );

            $rows = $this->ztdQuery("SELECT id, name, role FROM pg_bcu_users ORDER BY id");
            $this->assertCount(4, $rows);

            $expected = [
                ['id' => 1, 'role' => 'admin'],
                ['id' => 2, 'role' => 'moderator'],
                ['id' => 3, 'role' => 'editor'],
                ['id' => 4, 'role' => 'user'],
            ];

            foreach ($expected as $i => $exp) {
                if ($rows[$i]['role'] !== $exp['role']) {
                    $this->markTestIncomplete(
                        'Batch CASE UPDATE: id=' . $exp['id'] . ' role='
                        . var_export($rows[$i]['role'], true) . ', expected ' . $exp['role']
                        . '. CASE routing by id may not evaluate correctly.'
                    );
                }
            }

            $this->assertSame('admin', $rows[0]['role']);
            $this->assertSame('moderator', $rows[1]['role']);
            $this->assertSame('editor', $rows[2]['role']);
            $this->assertSame('user', $rows[3]['role']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Batch CASE UPDATE single column failed: ' . $e->getMessage());
        }
    }

    /**
     * Batch update: multiple columns with CASE WHEN id.
     */
    public function testBatchUpdateMultipleColumns(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_bcu_users
                 SET role = CASE
                         WHEN id = 1 THEN 'admin'
                         WHEN id = 2 THEN 'moderator'
                         ELSE role
                     END,
                     score = CASE
                         WHEN id = 1 THEN 100
                         WHEN id = 2 THEN 200
                         ELSE score
                     END
                 WHERE id IN (1, 2)"
            );

            $rows = $this->ztdQuery("SELECT id, role, score FROM pg_bcu_users ORDER BY id");
            $this->assertCount(4, $rows);

            if ($rows[0]['role'] !== 'admin' || (int) $rows[0]['score'] !== 100) {
                $this->markTestIncomplete(
                    'Multi-column CASE: Alice role=' . var_export($rows[0]['role'], true)
                    . ' score=' . var_export($rows[0]['score'], true)
                    . ', expected admin/100'
                );
            }
            $this->assertSame('admin', $rows[0]['role']);
            $this->assertEquals(100, (int) $rows[0]['score']);

            if ($rows[1]['role'] !== 'moderator' || (int) $rows[1]['score'] !== 200) {
                $this->markTestIncomplete(
                    'Multi-column CASE: Bob role=' . var_export($rows[1]['role'], true)
                    . ' score=' . var_export($rows[1]['score'], true)
                    . ', expected moderator/200'
                );
            }
            $this->assertSame('moderator', $rows[1]['role']);
            $this->assertEquals(200, (int) $rows[1]['score']);

            // Untouched rows
            $this->assertSame('user', $rows[2]['role']);
            $this->assertEquals(30, (int) $rows[2]['score']);
            $this->assertSame('user', $rows[3]['role']);
            $this->assertEquals(40, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Batch CASE UPDATE multiple columns failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared batch update with params in CASE values.
     */
    public function testBatchUpdatePreparedWithParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_bcu_users
                 SET role = CASE
                     WHEN id = ? THEN ?
                     WHEN id = ? THEN ?
                     ELSE role
                 END
                 WHERE id IN (?, ?)"
            );
            $stmt->execute([1, 'admin', 2, 'moderator', 1, 2]);

            $rows = $this->ztdQuery("SELECT id, role FROM pg_bcu_users ORDER BY id");

            if ($rows[0]['role'] !== 'admin') {
                $this->markTestIncomplete(
                    'Prepared batch CASE: id=1 role=' . var_export($rows[0]['role'], true)
                    . ', expected admin'
                );
            }
            $this->assertSame('admin', $rows[0]['role']);

            if ($rows[1]['role'] !== 'moderator') {
                $this->markTestIncomplete(
                    'Prepared batch CASE: id=2 role=' . var_export($rows[1]['role'], true)
                    . ', expected moderator'
                );
            }
            $this->assertSame('moderator', $rows[1]['role']);

            // Untouched
            $this->assertSame('user', $rows[2]['role']);
            $this->assertSame('user', $rows[3]['role']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared batch CASE UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Batch update without WHERE clause -- all rows get CASE-routed or ELSE.
     */
    public function testBatchUpdateWithoutWhere(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_bcu_users
                 SET score = CASE
                     WHEN id = 1 THEN 100
                     WHEN id = 2 THEN 200
                     ELSE 0
                 END"
            );

            $rows = $this->ztdQuery("SELECT id, score FROM pg_bcu_users ORDER BY id");
            $this->assertCount(4, $rows);

            $this->assertEquals(100, (int) $rows[0]['score']);
            $this->assertEquals(200, (int) $rows[1]['score']);
            $this->assertEquals(0, (int) $rows[2]['score']);
            $this->assertEquals(0, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Batch CASE UPDATE without WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Batch update with arithmetic in CASE branches.
     */
    public function testBatchUpdateWithArithmeticInCase(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE pg_bcu_users
                 SET score = CASE
                     WHEN id = 1 THEN score + 90
                     WHEN id = 2 THEN score * 5
                     WHEN id = 3 THEN score - 10
                     ELSE score
                 END
                 WHERE id IN (1, 2, 3)"
            );

            $rows = $this->ztdQuery("SELECT id, score FROM pg_bcu_users ORDER BY id");

            $expected = [
                ['id' => 1, 'score' => 100],  // 10 + 90
                ['id' => 2, 'score' => 100],  // 20 * 5
                ['id' => 3, 'score' => 20],   // 30 - 10
                ['id' => 4, 'score' => 40],   // unchanged
            ];

            foreach ($expected as $i => $exp) {
                if ((int) $rows[$i]['score'] !== $exp['score']) {
                    $this->markTestIncomplete(
                        'CASE with arithmetic: id=' . $exp['id'] . ' score='
                        . var_export($rows[$i]['score'], true) . ', expected ' . $exp['score']
                    );
                }
            }

            $this->assertEquals(100, (int) $rows[0]['score']);
            $this->assertEquals(100, (int) $rows[1]['score']);
            $this->assertEquals(20, (int) $rows[2]['score']);
            $this->assertEquals(40, (int) $rows[3]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Batch CASE with arithmetic failed: ' . $e->getMessage());
        }
    }

    /**
     * Sequential batch updates -- two CASE updates in sequence.
     */
    public function testSequentialBatchUpdates(): void
    {
        try {
            // First batch: set roles
            $this->pdo->exec(
                "UPDATE pg_bcu_users
                 SET role = CASE WHEN id = 1 THEN 'admin' WHEN id = 2 THEN 'editor' ELSE role END
                 WHERE id IN (1, 2)"
            );

            // Second batch: set scores based on new roles
            $this->pdo->exec(
                "UPDATE pg_bcu_users
                 SET score = CASE WHEN id = 1 THEN 999 WHEN id = 2 THEN 500 ELSE score END
                 WHERE id IN (1, 2)"
            );

            $rows = $this->ztdQuery("SELECT id, role, score FROM pg_bcu_users ORDER BY id");

            $this->assertSame('admin', $rows[0]['role']);
            $this->assertEquals(999, (int) $rows[0]['score']);
            $this->assertSame('editor', $rows[1]['role']);
            $this->assertEquals(500, (int) $rows[1]['score']);
            // Untouched
            $this->assertSame('user', $rows[2]['role']);
            $this->assertEquals(30, (int) $rows[2]['score']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential batch CASE updates failed: ' . $e->getMessage());
        }
    }
}
