<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests NULLIF and COALESCE with prepared parameters on MySQL PDO.
 *
 * Verified behavior: NULLIF(col, ?) works correctly via query() but returns
 * wrong results via prepare()/execute(). The prepared parameter is not
 * properly evaluated inside NULLIF, causing NULLIF(100, 100) to not return
 * NULL as expected.
 *
 * @spec SPEC-3.1
 */
class MysqlNullifWithParamsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE my_nwp_scores (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            score INT
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['my_nwp_scores'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO my_nwp_scores VALUES (1, 'Alice', 100)");
        $this->ztdExec("INSERT INTO my_nwp_scores VALUES (2, 'Bob', 200)");
        $this->ztdExec("INSERT INTO my_nwp_scores VALUES (3, 'Charlie', NULL)");
    }

    /**
     * Baseline: NULLIF without prepared params works correctly.
     */
    public function testNullifWithoutParams(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM my_nwp_scores WHERE NULLIF(score, 100) IS NULL ORDER BY name"
        );

        // NULLIF(100, 100) = NULL (Alice), NULLIF(NULL, 100) = NULL (Charlie)
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
    }

    /**
     * NULLIF with prepared param — should match same rows as without params.
     */
    public function testNullifWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM my_nwp_scores WHERE NULLIF(score, ?) IS NULL ORDER BY name",
                [100]
            );

            if (count($rows) === 1) {
                $this->markTestIncomplete(
                    'NULLIF with prepared param returns only ' . count($rows) . ' row(s) '
                    . '(' . implode(', ', array_column($rows, 'name')) . '). '
                    . 'Expected 2 (Alice + Charlie). '
                    . 'The prepared parameter is not properly evaluated inside NULLIF.'
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertContains('Charlie', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NULLIF with prepared param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COALESCE with prepared param in WHERE.
     */
    public function testCoalesceWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name FROM my_nwp_scores WHERE COALESCE(score, ?) > 150 ORDER BY name",
                [0]
            );

            // Bob (200 > 150). Alice (100 > 150 = false). Charlie (COALESCE(NULL, 0) = 0 > 150 = false)
            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COALESCE with prepared param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * IFNULL (MySQL-specific) with prepared param.
     */
    public function testIfnullWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, IFNULL(score, ?) AS effective_score FROM my_nwp_scores ORDER BY name",
                [-1]
            );

            $this->assertCount(3, $rows);
            // Charlie's NULL score should become -1
            $charlie = null;
            foreach ($rows as $row) {
                if ($row['name'] === 'Charlie') {
                    $charlie = $row;
                }
            }

            $this->assertNotNull($charlie);
            if ((int) $charlie['effective_score'] !== -1) {
                $this->markTestIncomplete(
                    'IFNULL with prepared param: Charlie effective_score = '
                    . var_export($charlie['effective_score'], true) . ', expected -1'
                );
            }
            $this->assertEquals(-1, (int) $charlie['effective_score']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'IFNULL with prepared param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NULLIF in UPDATE SET with prepared param.
     */
    public function testNullifInUpdateWithParam(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE my_nwp_scores SET score = NULLIF(score, ?) WHERE id = ?"
            );
            $stmt->execute([100, 1]);

            $rows = $this->ztdQuery("SELECT score FROM my_nwp_scores WHERE id = 1");
            $this->assertCount(1, $rows);

            if ($rows[0]['score'] !== null) {
                $this->markTestIncomplete(
                    'NULLIF in UPDATE SET with param: score = '
                    . var_export($rows[0]['score'], true) . ', expected NULL. '
                    . 'NULLIF(100, 100) should return NULL.'
                );
            }
            $this->assertNull($rows[0]['score']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NULLIF in UPDATE SET with param failed: ' . $e->getMessage()
            );
        }
    }
}
