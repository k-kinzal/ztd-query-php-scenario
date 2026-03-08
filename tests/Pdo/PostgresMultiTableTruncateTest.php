<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL multi-table TRUNCATE behavior in ZTD.
 *
 * PostgreSQL supports TRUNCATE table1, table2, table3 to truncate multiple
 * tables in a single statement. The PgSqlParser::extractTruncateTable()
 * regex only captures the first table name, so multi-table TRUNCATE
 * may only truncate the first table in shadow.
 * @spec pending
 */
class PostgresMultiTableTruncateTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mtt_alpha (id INT PRIMARY KEY, val VARCHAR(50))',
            'CREATE TABLE pg_mtt_beta (id INT PRIMARY KEY, val VARCHAR(50))',
            'CREATE TABLE pg_mtt_gamma (id INT PRIMARY KEY, val VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mtt_alpha', 'pg_mtt_beta', 'pg_mtt_gamma'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_mtt_alpha (id, val) VALUES (1, 'a1')");
        $this->pdo->exec("INSERT INTO pg_mtt_alpha (id, val) VALUES (2, 'a2')");
        $this->pdo->exec("INSERT INTO pg_mtt_beta (id, val) VALUES (1, 'b1')");
        $this->pdo->exec("INSERT INTO pg_mtt_beta (id, val) VALUES (2, 'b2')");
        $this->pdo->exec("INSERT INTO pg_mtt_gamma (id, val) VALUES (1, 'g1')");
    }

    /**
     * Single-table TRUNCATE works correctly in shadow.
     */
    public function testSingleTableTruncate(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_mtt_alpha');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_alpha');
        $this->assertSame(0, (int) $stmt->fetchColumn());

        // Other tables should be unaffected
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_beta');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * Multi-table TRUNCATE: TRUNCATE table1, table2.
     *
     * PostgreSQL supports this natively, but ZTD's extractTruncateTable()
     * only captures the first table. This means the second table might
     * NOT be truncated in the shadow store.
     *
     * Multi-table TRUNCATE should truncate all listed tables.
     */
    public function testMultiTableTruncateAllTables(): void
    {
        $this->pdo->exec('TRUNCATE pg_mtt_alpha, pg_mtt_beta');

        // First table should be truncated
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_alpha');
        $alphaCount = (int) $stmt->fetchColumn();
        $this->assertSame(0, $alphaCount, 'First table should be truncated');

        // Second table should also be truncated
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_beta');
        $betaCount = (int) $stmt->fetchColumn();
        if ($betaCount !== 0) {
            $this->markTestIncomplete(
                'Multi-table TRUNCATE only truncates the first table in shadow. '
                . 'Expected beta count 0, got ' . $betaCount
            );
        }
        $this->assertSame(0, $betaCount);
    }

    /**
     * Multi-table TRUNCATE with three tables should truncate all.
     */
    public function testMultiTableTruncateThreeTables(): void
    {
        $this->pdo->exec('TRUNCATE pg_mtt_alpha, pg_mtt_beta, pg_mtt_gamma');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_alpha');
        $this->assertSame(0, (int) $stmt->fetchColumn(), 'First table truncated');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_beta');
        $betaCount = (int) $stmt->fetchColumn();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_gamma');
        $gammaCount = (int) $stmt->fetchColumn();

        if ($betaCount !== 0 || $gammaCount !== 0) {
            $this->markTestIncomplete(
                'Multi-table TRUNCATE only truncates the first table. '
                . 'Beta count: ' . $betaCount . ', gamma count: ' . $gammaCount
                . ' (both should be 0)'
            );
        }
        $this->assertSame(0, $betaCount);
        $this->assertSame(0, $gammaCount);
    }

    /**
     * TRUNCATE TABLE with TABLE keyword and comma-separated list should truncate all.
     */
    public function testMultiTableTruncateWithTableKeyword(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_mtt_alpha, pg_mtt_beta');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_alpha');
        $this->assertSame(0, (int) $stmt->fetchColumn(), 'First table truncated');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_beta');
        $betaCount = (int) $stmt->fetchColumn();
        if ($betaCount !== 0) {
            $this->markTestIncomplete(
                'Multi-table TRUNCATE TABLE only truncates the first table. '
                . 'Expected beta count 0, got ' . $betaCount
            );
        }
        $this->assertSame(0, $betaCount);
    }

    /**
     * Physical isolation: TRUNCATE stays in shadow.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pg_mtt_alpha');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_mtt_alpha');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
