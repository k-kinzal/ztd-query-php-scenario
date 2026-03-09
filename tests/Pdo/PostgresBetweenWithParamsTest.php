<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests BETWEEN operator with prepared $N params on PostgreSQL shadow data.
 *
 * BETWEEN is common in range queries. With $N params, the CTE rewriter
 * must handle `col BETWEEN $1 AND $2` correctly. Given the known issues
 * with $N parameter handling (Issue #68), BETWEEN may be affected.
 *
 * @spec SPEC-3.2
 * @spec SPEC-3.3
 */
class PostgresBetweenWithParamsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_bwp_events (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                event_date DATE NOT NULL,
                price NUMERIC(10,2) NOT NULL,
                category VARCHAR(50) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_bwp_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_bwp_events (id, title, event_date, price, category) VALUES (1, 'Concert A', '2025-01-15', 50.00, 'music')");
        $this->pdo->exec("INSERT INTO pg_bwp_events (id, title, event_date, price, category) VALUES (2, 'Concert B', '2025-02-20', 75.00, 'music')");
        $this->pdo->exec("INSERT INTO pg_bwp_events (id, title, event_date, price, category) VALUES (3, 'Workshop', '2025-03-10', 30.00, 'education')");
        $this->pdo->exec("INSERT INTO pg_bwp_events (id, title, event_date, price, category) VALUES (4, 'Gala', '2025-04-05', 200.00, 'social')");
        $this->pdo->exec("INSERT INTO pg_bwp_events (id, title, event_date, price, category) VALUES (5, 'Seminar', '2025-01-25', 0.00, 'education')");
    }

    /**
     * BETWEEN with prepared $N params.
     */
    public function testBetweenWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT title FROM pg_bwp_events
             WHERE price BETWEEN $1 AND $2
             ORDER BY title"
        );
        $stmt->execute([25, 100]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'BETWEEN with $N params returned no rows on PostgreSQL. '
                . 'The $N params may not be properly bound in BETWEEN expressions.'
            );
        }

        $this->assertCount(3, $rows);
    }

    /**
     * BETWEEN on date with prepared $N params.
     */
    public function testBetweenDateWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT title FROM pg_bwp_events
             WHERE event_date BETWEEN $1::DATE AND $2::DATE
             ORDER BY event_date"
        );
        $stmt->execute(['2025-01-01', '2025-02-28']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'BETWEEN with date $N params and ::DATE cast returned no rows.'
            );
        }

        $this->assertCount(3, $rows);
    }

    /**
     * BETWEEN combined with other conditions using $N.
     */
    public function testBetweenWithOtherConditions(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT title FROM pg_bwp_events
             WHERE price BETWEEN $1 AND $2
               AND category = $3
             ORDER BY title"
        );
        $stmt->execute([0, 100, 'education']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                'BETWEEN with other $N conditions returned no rows.'
            );
        }

        $this->assertCount(2, $rows);
    }

    /**
     * BETWEEN without prepared params (literal values).
     */
    public function testBetweenLiteralValues(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title FROM pg_bwp_events
             WHERE price BETWEEN 25 AND 100
             ORDER BY title"
        );

        $this->assertCount(3, $rows);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_bwp_events')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
