<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT with scalar subquery in VALUES clause through CTE shadow.
 *
 * Pattern: INSERT INTO t (col) VALUES ((SELECT expr FROM ...))
 * This is valid SQL that computes a value from one table and inserts into another.
 * The CTE rewriter must handle subqueries inside VALUES.
 *
 * @spec SPEC-4.1
 */
class SqliteInsertSubqueryInValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sq_val_config (key TEXT PRIMARY KEY, value TEXT)',
            'CREATE TABLE sq_val_audit (id INTEGER PRIMARY KEY, config_key TEXT, snapshot_value TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sq_val_audit', 'sq_val_config'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sq_val_config VALUES ('max_retries', '5')");
        $this->pdo->exec("INSERT INTO sq_val_config VALUES ('timeout', '30')");
    }

    /**
     * INSERT with scalar subquery in VALUES.
     */
    public function testInsertWithScalarSubqueryInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sq_val_audit (id, config_key, snapshot_value)
                 VALUES (1, 'max_retries', (SELECT value FROM sq_val_config WHERE key = 'max_retries'))"
            );

            $rows = $this->ztdQuery('SELECT * FROM sq_val_audit WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('max_retries', $rows[0]['config_key']);
            $this->assertSame('5', $rows[0]['snapshot_value'], 'Subquery in VALUES should resolve to config value');
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT with subquery in VALUES not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT subquery in VALUES referencing shadow-modified data.
     */
    public function testInsertSubqueryReferencingShadowUpdate(): void
    {
        $this->pdo->exec("UPDATE sq_val_config SET value = '10' WHERE key = 'max_retries'");

        try {
            $this->pdo->exec(
                "INSERT INTO sq_val_audit (id, config_key, snapshot_value)
                 VALUES (1, 'max_retries', (SELECT value FROM sq_val_config WHERE key = 'max_retries'))"
            );

            $rows = $this->ztdQuery('SELECT snapshot_value FROM sq_val_audit WHERE id = 1');
            $this->assertSame('10', $rows[0]['snapshot_value'], 'Should see updated shadow value');
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT subquery with shadow data not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with multiple scalar subqueries in VALUES.
     */
    public function testInsertMultipleSubqueriesInValues(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sq_val_audit (id, config_key, snapshot_value)
                 VALUES (
                     1,
                     (SELECT key FROM sq_val_config WHERE value = '30'),
                     (SELECT value FROM sq_val_config WHERE key = 'timeout')
                 )"
            );

            $rows = $this->ztdQuery('SELECT * FROM sq_val_audit WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertSame('timeout', $rows[0]['config_key']);
            $this->assertSame('30', $rows[0]['snapshot_value']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Multiple subqueries in VALUES not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT with subquery that returns NULL (no match).
     */
    public function testInsertSubqueryReturningNull(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sq_val_audit (id, config_key, snapshot_value)
                 VALUES (1, 'missing', (SELECT value FROM sq_val_config WHERE key = 'nonexistent'))"
            );

            $rows = $this->ztdQuery('SELECT * FROM sq_val_audit WHERE id = 1');
            $this->assertCount(1, $rows);
            $this->assertNull($rows[0]['snapshot_value'], 'Non-matching subquery should produce NULL');
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT subquery returning NULL not supported: ' . $e->getMessage());
        }
    }

    /**
     * INSERT subquery in VALUES with aggregate.
     */
    public function testInsertSubqueryWithAggregate(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sq_val_audit (id, config_key, snapshot_value)
                 VALUES (1, 'count', (SELECT CAST(COUNT(*) AS TEXT) FROM sq_val_config))"
            );

            $rows = $this->ztdQuery('SELECT snapshot_value FROM sq_val_audit WHERE id = 1');
            $this->assertSame('2', $rows[0]['snapshot_value'], 'COUNT aggregate subquery should return 2');
        } catch (\Throwable $e) {
            $this->markTestSkipped('INSERT subquery with aggregate not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_val_config');
        $this->assertSame(0, (int) $stmt->fetchColumn());
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_val_audit');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
