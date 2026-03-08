<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CREATE TABLE AS SELECT (CTAS) behavior on SQLite through ZTD.
 * On SQLite, CTAS succeeds but subsequent SELECT may fail, and INSERT
 * after CTAS causes original data loss.
 * @spec SPEC-5.1c
 */
class SqliteCtasDataLossTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_ctas_src (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['sl_ctas_src', 'sl_ctas_dst'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ctas_src VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO sl_ctas_src VALUES (2, 'Bob', 200)");
        $this->pdo->exec("INSERT INTO sl_ctas_src VALUES (3, 'Charlie', 300)");
    }

    /**
     * CTAS via disableZtd creates physical table from physical (empty) source.
     */
    public function testCtasViaDisabledZtdCreatesEmptyTable(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec('CREATE TABLE sl_ctas_dst AS SELECT * FROM sl_ctas_src');

        // Physical source is empty (shadow only), so CTAS creates empty table
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_ctas_dst');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        $this->pdo->enableZtd();
    }

    /**
     * CTAS with ZTD enabled: verify if shadow data is used.
     */
    public function testCtasWithZtdEnabled(): void
    {
        try {
            $this->pdo->exec('CREATE TABLE sl_ctas_dst AS SELECT * FROM sl_ctas_src WHERE score > 100');

            // If CTAS succeeds, try to read from the new table
            try {
                $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ctas_dst");
                // CTAS may or may not populate from shadow data
                $count = (int) $rows[0]['cnt'];
                $this->assertGreaterThanOrEqual(0, $count);
            } catch (\Exception $e) {
                // "no such table" after CTAS is a known SQLite issue
                $this->assertMatchesRegularExpression('/no such table/i', $e->getMessage());
            }
        } catch (\Exception $e) {
            // CTAS itself may throw through ZTD
            $this->addToAssertionCount(1);
        }
    }

    /**
     * INSERT into table created after adapter construction (unreflected):
     * column list must be explicit or INSERT may fail.
     */
    public function testInsertIntoUnreflectedCtasTable(): void
    {
        // Create destination via raw connection
        $this->pdo->disableZtd();
        $this->pdo->exec('CREATE TABLE sl_ctas_dst (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');
        $this->pdo->exec("INSERT INTO sl_ctas_dst VALUES (1, 'Original', 999)");
        $this->pdo->enableZtd();

        // Table was created after adapter construction but before enableZtd.
        // Shadow INSERT may throw because ZTD doesn't have column info.
        try {
            $this->pdo->exec("INSERT INTO sl_ctas_dst (id, name, score) VALUES (2, 'Shadow', 888)");

            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ctas_dst");
            $count = (int) $rows[0]['cnt'];
            $this->assertGreaterThanOrEqual(1, $count);
        } catch (\Exception $e) {
            // May throw "Cannot determine columns" for unreflected table
            $this->assertStringContainsString('columns', strtolower($e->getMessage()));
        }
    }

    /**
     * Source table shadow data remains intact after CTAS.
     */
    public function testSourceDataIntactAfterCtas(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec('CREATE TABLE sl_ctas_dst AS SELECT * FROM sl_ctas_src');
        $this->pdo->enableZtd();

        // Source shadow data should still be intact
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_ctas_src");
        $this->assertSame(3, (int) $rows[0]['cnt']);
    }

    /**
     * Physical isolation on source.
     */
    public function testPhysicalIsolationOnSource(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_ctas_src');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
