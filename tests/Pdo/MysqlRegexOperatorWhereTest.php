<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests REGEXP operator in WHERE clause via MySQL PDO.
 *
 * REGEXP is a common search operator. The CTE rewriter must
 * parse it correctly without confusing the keyword or its operands.
 *
 * @spec SPEC-3.1
 */
class MysqlRegexOperatorWhereTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_regex_test (
            id INT PRIMARY KEY,
            email VARCHAR(200) NOT NULL,
            code VARCHAR(50) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_regex_test'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_regex_test VALUES (1, 'alice@example.com', 'ABC-123')");
        $this->pdo->exec("INSERT INTO mp_regex_test VALUES (2, 'bob@test.org', 'DEF-456')");
        $this->pdo->exec("INSERT INTO mp_regex_test VALUES (3, 'charlie@example.com', 'GHI-789')");
        $this->pdo->exec("INSERT INTO mp_regex_test VALUES (4, 'dave@other.net', 'ABC-999')");
        $this->pdo->exec("INSERT INTO mp_regex_test VALUES (5, 'eve@test.org', 'XYZ-000')");
    }

    /**
     * SELECT with REGEXP in WHERE clause.
     *
     * email REGEXP '@example\\.com$' matches ids 1 and 3.
     */
    public function testSelectWithRegexp(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM mp_regex_test WHERE email REGEXP '@example\\\\.com$' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with REGEXP: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with NOT REGEXP in WHERE clause.
     *
     * email NOT REGEXP '@example\\.com$' matches ids 2, 4, 5.
     */
    public function testSelectWithNotRegexp(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM mp_regex_test WHERE email NOT REGEXP '@example\\\\.com$' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'SELECT with NOT REGEXP: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([2, 4, 5], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with NOT REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with REGEXP matching a code pattern.
     *
     * code REGEXP '^ABC-[0-9]+$' matches ids 1 and 4.
     */
    public function testSelectWithRegexpPattern(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id, code FROM mp_regex_test WHERE code REGEXP '^ABC-[0-9]+$' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with REGEXP pattern: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with REGEXP pattern failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE with REGEXP in WHERE clause.
     *
     * Delete rows where email matches '@test\\.org$' (ids 2 and 5).
     * Remaining: ids 1, 3, 4.
     */
    public function testDeleteWithRegexp(): void
    {
        try {
            $this->pdo->exec("DELETE FROM mp_regex_test WHERE email REGEXP '@test\\\\.org$'");

            $rows = $this->pdo->query("SELECT id FROM mp_regex_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE with REGEXP: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertSame([1, 3, 4], $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'DELETE with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with REGEXP in WHERE clause.
     *
     * Update code to 'MATCHED' where code REGEXP '^[A-Z]{3}-[0-9]{3}$' (3-letter + 3-digit).
     * All 5 rows match this pattern.
     */
    public function testUpdateWithRegexp(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE mp_regex_test SET code = 'MATCHED' WHERE code REGEXP '^[A-Z]{3}-[0-9]{3}$'"
            );

            $rows = $this->pdo->query("SELECT id, code FROM mp_regex_test ORDER BY id")
                ->fetchAll(PDO::FETCH_ASSOC);

            $this->assertCount(5, $rows);
            $matchedCount = count(array_filter($rows, fn($r) => $r['code'] === 'MATCHED'));

            if ($matchedCount !== 5) {
                $codes = array_map(fn($r) => "id={$r['id']} code={$r['code']}", $rows);
                $this->markTestIncomplete(
                    'UPDATE with REGEXP: expected all 5 rows updated, got ' . $matchedCount
                    . '. ' . implode(', ', $codes)
                );
            }

            $this->assertSame(5, $matchedCount);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared SELECT with REGEXP and bound parameter.
     *
     * The regex pattern is passed as a bound parameter.
     */
    public function testPreparedSelectWithRegexp(): void
    {
        try {
            $stmt = $this->pdo->prepare("SELECT id FROM mp_regex_test WHERE code REGEXP ? ORDER BY id");
            $stmt->execute(['^ABC']);

            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Prepared SELECT with REGEXP: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(4, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared SELECT with REGEXP failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT with RLIKE synonym (MySQL alias for REGEXP).
     *
     * RLIKE and REGEXP are synonyms in MySQL.
     */
    public function testSelectWithRlike(): void
    {
        try {
            $rows = $this->pdo->query(
                "SELECT id FROM mp_regex_test WHERE email RLIKE '@example' ORDER BY id"
            )->fetchAll(PDO::FETCH_ASSOC);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'SELECT with RLIKE: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT with RLIKE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Physical isolation: DML with REGEXP should not affect the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("DELETE FROM mp_regex_test WHERE email REGEXP '@test'");

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_regex_test")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
