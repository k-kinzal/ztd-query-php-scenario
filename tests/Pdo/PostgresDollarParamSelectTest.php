<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL $N parameter syntax in basic SELECT queries.
 *
 * Demonstrates that PostgreSQL's native numbered placeholder syntax ($1, $2)
 * fails in SELECT WHERE clauses through the CTE rewriter, while the same
 * queries with ? placeholders work correctly. This is the most basic and
 * common SELECT pattern.
 *
 * Related upstream issues: #62 (FILTER+$N), #63 (USING+$N), #68 (INSERT+$N),
 * #85 (SELECT+$N — this issue).
 * This test documents the broader scope: $N params fail in ALL SELECT contexts.
 *
 * @spec SPEC-3.2
 */
class PostgresDollarParamSelectTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_dps_items (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                category VARCHAR(50) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_dps_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert via exec() — known to work correctly
        $this->pdo->exec("INSERT INTO pg_dps_items (id, name, price, category) VALUES (1, 'Widget', 29.99, 'tools')");
        $this->pdo->exec("INSERT INTO pg_dps_items (id, name, price, category) VALUES (2, 'Gadget', 149.99, 'electronics')");
        $this->pdo->exec("INSERT INTO pg_dps_items (id, name, price, category) VALUES (3, 'Book', 19.99, 'media')");
    }

    /**
     * Verify data is present via query() (no params).
     */
    public function testDataPresentViaQuery(): void
    {
        $rows = $this->ztdQuery("SELECT * FROM pg_dps_items ORDER BY id");
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }

    /**
     * SELECT WHERE id = ? (question mark placeholder) — works correctly.
     */
    public function testSelectWithQuestionMarkParam(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM pg_dps_items WHERE id = ?",
            [1]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }

    /**
     * SELECT WHERE id = $1 (dollar placeholder) — fails.
     *
     * This is the most basic SELECT pattern on PostgreSQL. The $1 placeholder
     * is PostgreSQL's native parameter syntax and is used by many PostgreSQL
     * client libraries and ORMs.
     */
    public function testSelectWithDollarParam(): void
    {
        $stmt = $this->pdo->prepare("SELECT name FROM pg_dps_items WHERE id = $1");
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'SELECT WHERE id = $1 returned no rows on PostgreSQL, '
                . 'but SELECT WHERE id = ? returns correct results. '
                . 'The CTE rewriter does not properly bind $N parameters in SELECT queries. '
                . 'This affects all SELECT patterns using $N syntax. '
                . 'Workaround: use ? placeholders instead of $N.'
            );
        }

        $this->assertCount(1, $rows);
        $this->assertSame('Widget', $rows[0]['name']);
    }

    /**
     * SELECT with multiple $N params — fails.
     */
    public function testSelectWithMultipleDollarParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT name FROM pg_dps_items WHERE price > $1 AND category = $2 ORDER BY name"
        );
        $stmt->execute([20, 'electronics']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'SELECT with multiple $N params returned no rows. '
                . '$N params are not properly bound in SELECT WHERE clauses.'
            );
        }

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
    }

    /**
     * SELECT with multiple ? params — works (control test).
     */
    public function testSelectWithMultipleQuestionMarkParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM pg_dps_items WHERE price > ? AND category = ? ORDER BY name",
            [20, 'electronics']
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['name']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_dps_items')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
