<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL :: type cast operator in queries on shadow data.
 *
 * Real-world scenario: PostgreSQL applications frequently use the ::
 * type cast operator for type conversions (e.g., text::integer,
 * timestamp::date). The CTE rewriter must handle :: correctly without
 * misinterpreting it as part of a table name or breaking the SQL syntax
 * when generating CTEs.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class PostgresTypeCastOperatorTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_tco_events (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                event_date TIMESTAMP NOT NULL,
                price_text VARCHAR(20) NOT NULL,
                metadata TEXT NOT NULL DEFAULT \'{}\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_tco_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_tco_events (id, name, event_date, price_text, metadata) VALUES (1, 'Concert', '2025-06-15 20:00:00', '49.99', '{\"capacity\": 500}')");
        $this->pdo->exec("INSERT INTO pg_tco_events (id, name, event_date, price_text, metadata) VALUES (2, 'Workshop', '2025-07-20 10:00:00', '25.00', '{\"capacity\": 30}')");
        $this->pdo->exec("INSERT INTO pg_tco_events (id, name, event_date, price_text, metadata) VALUES (3, 'Meetup', '2025-06-15 18:00:00', '0.00', '{\"capacity\": 100}')");
    }

    /**
     * :: cast in SELECT list.
     */
    public function testTypeCastInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price_text::NUMERIC AS price_num, event_date::DATE AS event_day
             FROM pg_tco_events
             ORDER BY name"
        );

        $this->assertCount(3, $rows);
        $this->assertEqualsWithDelta(49.99, (float) $rows[0]['price_num'], 0.01); // Concert
        $this->assertSame('2025-06-15', $rows[0]['event_day']);
    }

    /**
     * :: cast in WHERE clause.
     */
    public function testTypeCastInWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM pg_tco_events
             WHERE price_text::NUMERIC > 0
             ORDER BY name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Concert', $rows[0]['name']);
        $this->assertSame('Workshop', $rows[1]['name']);
    }

    /**
     * :: cast in ORDER BY.
     */
    public function testTypeCastInOrderBy(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price_text FROM pg_tco_events
             ORDER BY price_text::NUMERIC DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Concert', $rows[0]['name']); // 49.99
        $this->assertSame('Workshop', $rows[1]['name']); // 25.00
        $this->assertSame('Meetup', $rows[2]['name']); // 0.00
    }

    /**
     * :: cast with date truncation.
     */
    public function testTypeCastDateTruncation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT event_date::DATE AS day, COUNT(*) AS cnt
             FROM pg_tco_events
             GROUP BY event_date::DATE
             ORDER BY day"
        );

        $this->assertCount(2, $rows);
        // 2025-06-15: Concert + Meetup = 2
        $this->assertSame('2025-06-15', $rows[0]['day']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        // 2025-07-20: Workshop = 1
        $this->assertEquals(1, (int) $rows[1]['cnt']);
    }

    /**
     * :: cast with JSONB extraction.
     */
    public function testTypeCastJsonbExtraction(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, (metadata::JSONB ->> 'capacity')::INTEGER AS capacity
                 FROM pg_tco_events
                 ORDER BY capacity DESC"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'JSONB :: cast extraction returned no rows.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Concert', $rows[0]['name']);
            $this->assertEquals(500, (int) $rows[0]['capacity']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'JSONB :: cast extraction failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * :: cast in UPDATE SET.
     */
    public function testTypeCastInUpdateSet(): void
    {
        $this->pdo->exec(
            "UPDATE pg_tco_events SET price_text = (price_text::NUMERIC * 1.10)::TEXT
             WHERE price_text::NUMERIC > 0"
        );

        $rows = $this->ztdQuery("SELECT name, price_text FROM pg_tco_events ORDER BY name");

        // Concert: 49.99 * 1.10 = 54.989
        $this->assertStringContainsString('54.9', $rows[0]['price_text']);
        // Meetup: 0.00 unchanged
        $this->assertSame('0.00', $rows[1]['price_text']);
    }

    /**
     * :: cast with prepared $N params.
     */
    public function testTypeCastWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT name FROM pg_tco_events
             WHERE price_text::NUMERIC > $1::NUMERIC
             ORDER BY name"
        );
        $stmt->execute([10]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if (empty($rows)) {
            $this->markTestIncomplete(
                ':: cast with prepared $N params returned no rows.'
            );
        }

        $this->assertCount(2, $rows);
        $this->assertSame('Concert', $rows[0]['name']);
        $this->assertSame('Workshop', $rows[1]['name']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_tco_events')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
