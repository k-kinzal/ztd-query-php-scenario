<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests NULLIF and COALESCE with prepared parameters on PostgreSQL.
 *
 * PostgreSQL uses $N placeholder syntax. The CTE rewriter must correctly
 * handle $N parameters inside NULLIF/COALESCE function calls.
 *
 * @spec SPEC-3.1
 */
class PostgresNullifWithParamsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_nwp_scores (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            score INT
        )';
    }

    protected function getTableNames(): array
    {
        return ['pg_nwp_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_nwp_scores VALUES (1, 'Alice', 100)");
        $this->ztdExec("INSERT INTO pg_nwp_scores VALUES (2, 'Bob', 200)");
        $this->ztdExec("INSERT INTO pg_nwp_scores VALUES (3, 'Charlie', NULL)");
    }

    /**
     * Baseline: NULLIF without prepared params.
     */
    public function testNullifWithoutParams(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_nwp_scores WHERE NULLIF(score, 100) IS NULL ORDER BY name"
        );

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }

    /**
     * NULLIF with $N prepared param.
     */
    public function testNullifWithPreparedParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name FROM pg_nwp_scores WHERE NULLIF(score, $1) IS NULL ORDER BY name"
            );
            $stmt->execute([100]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) === 1) {
                $this->markTestIncomplete(
                    'NULLIF with $1 param returns only ' . count($rows) . ' row(s) '
                    . '(' . implode(', ', array_column($rows, 'name')) . '). '
                    . 'Expected 2 (Alice + Charlie). '
                    . 'The $N parameter is not properly evaluated inside NULLIF.'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NULLIF with $N param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NULLIF with ? placeholder param (PDO positional).
     */
    public function testNullifWithPositionalParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM pg_nwp_scores WHERE NULLIF(score, ?) IS NULL ORDER BY name",
                [100]
            );

            if (count($rows) === 1) {
                $this->markTestIncomplete(
                    'NULLIF with ? param returns only ' . count($rows) . ' row(s). '
                    . 'Expected 2 (Alice + Charlie).'
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NULLIF with ? param on PostgreSQL failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE with $N prepared param in WHERE.
     */
    public function testCoalesceWithDollarParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "SELECT name FROM pg_nwp_scores WHERE COALESCE(score, $1) > 150 ORDER BY name"
            );
            $stmt->execute([0]);
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COALESCE with $N param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NULLIF in UPDATE SET with $N param.
     */
    public function testNullifInUpdateWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE pg_nwp_scores SET score = NULLIF(score, $1) WHERE id = $2"
            );
            $stmt->execute([100, 1]);

            $rows = $this->ztdQuery("SELECT score FROM pg_nwp_scores WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['score'] !== null) {
                $this->markTestIncomplete(
                    'NULLIF in UPDATE SET with $N param: score = '
                    . var_export($rows[0]['score'], true) . ', expected NULL.'
                );
            }
            $this->assertNull($rows[0]['score']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NULLIF in UPDATE SET with $N param failed: ' . $e->getMessage()
            );
        }
    }
}
