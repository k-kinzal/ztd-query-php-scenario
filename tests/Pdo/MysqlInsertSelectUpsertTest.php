<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT ... SELECT ... ON DUPLICATE KEY UPDATE on MySQL PDO ZTD.
 *
 * This is a MySQL-specific pattern combining INSERT...SELECT with upsert
 * behavior. The mutation resolver creates UpsertMutation when onDuplicateSet
 * is detected, while InsertTransformer::buildInsertFromSelect() wraps the
 * inner SELECT in a subquery for CTE shadowing.
 * @spec SPEC-4.1a
 */
class MysqlInsertSelectUpsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pdo_isu_source (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
            'CREATE TABLE pdo_isu_target (id INT PRIMARY KEY, name VARCHAR(50), score INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pdo_isu_target', 'pdo_isu_source'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_isu_source (id, name, score) VALUES (1, 'Alice', 90)");
        $this->pdo->exec("INSERT INTO pdo_isu_source (id, name, score) VALUES (2, 'Bob', 80)");
        $this->pdo->exec("INSERT INTO pdo_isu_source (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    /**
     * INSERT ... SELECT ... ON DUPLICATE KEY UPDATE — all new rows.
     */
    public function testInsertSelectOnDuplicateKeyUpdateAllNew(): void
    {
        $this->pdo->exec(
            "INSERT INTO pdo_isu_target (id, name, score) "
            . "SELECT id, name, score FROM pdo_isu_source "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_isu_target');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM pdo_isu_target WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * INSERT ... SELECT ... ON DUPLICATE KEY UPDATE — with conflicts.
     */
    public function testInsertSelectOnDuplicateKeyUpdateWithConflict(): void
    {
        $this->pdo->exec("INSERT INTO pdo_isu_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->pdo->exec(
            "INSERT INTO pdo_isu_target (id, name, score) "
            . "SELECT id, name, score FROM pdo_isu_source "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_isu_target');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name, score FROM pdo_isu_target WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(90, (int) $row['score']);
    }

    /**
     * INSERT ... SELECT with WHERE ... ON DUPLICATE KEY UPDATE.
     */
    public function testInsertSelectWithWhereOnDuplicateKeyUpdate(): void
    {
        $this->pdo->exec("INSERT INTO pdo_isu_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->pdo->exec(
            "INSERT INTO pdo_isu_target (id, name, score) "
            . "SELECT id, name, score FROM pdo_isu_source WHERE score >= 80 "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_isu_target');
        $this->assertSame(2, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT name FROM pdo_isu_target WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * INSERT ... SELECT ... ON DUPLICATE KEY UPDATE with expression in SET.
     *
     * Limitation: UpsertMutation can only handle simple VALUES(col) or literal
     * references. Expressions like "score = score + VALUES(score)" are stored
     * as raw strings, not evaluated.
     */
    public function testInsertSelectOnDuplicateKeyUpdateWithExpressionStoresLiteral(): void
    {
        $this->pdo->exec("INSERT INTO pdo_isu_target (id, name, score) VALUES (1, 'Old_Alice', 50)");

        $this->pdo->exec(
            "INSERT INTO pdo_isu_target (id, name, score) "
            . "SELECT id, name, score FROM pdo_isu_source WHERE id = 1 "
            . "ON DUPLICATE KEY UPDATE score = score + VALUES(score)"
        );

        // Expected: 140 (50 + 90), Actual: expression string stored literally
        $stmt = $this->pdo->query('SELECT score FROM pdo_isu_target WHERE id = 1');
        $score = $stmt->fetchColumn();
        $this->assertSame(0, (int) $score, 'Expression in ON DUPLICATE KEY UPDATE is not evaluated — stored as literal');
    }

    /**
     * Physical isolation: INSERT...SELECT...ON DUPLICATE KEY UPDATE stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec(
            "INSERT INTO pdo_isu_target (id, name, score) "
            . "SELECT id, name, score FROM pdo_isu_source "
            . "ON DUPLICATE KEY UPDATE name = VALUES(name), score = VALUES(score)"
        );

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_isu_target');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
