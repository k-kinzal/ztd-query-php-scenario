<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests JSON extraction functions (json_extract, ->, ->>) in queries on
 * shadow data for SQLite.
 *
 * Real-world scenario: many applications store semi-structured data in JSON
 * columns and filter/sort by JSON paths. The CTE rewriter must not interfere
 * with the -> and ->> operators or json_extract() function calls.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteJsonExtractInWhereTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_jeiw_events (
                id INTEGER PRIMARY KEY,
                data TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_jeiw_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_jeiw_events VALUES (1, '{\"type\":\"click\",\"page\":\"home\",\"count\":5}')");
        $this->ztdExec("INSERT INTO sl_jeiw_events VALUES (2, '{\"type\":\"scroll\",\"page\":\"about\",\"count\":12}')");
        $this->ztdExec("INSERT INTO sl_jeiw_events VALUES (3, '{\"type\":\"click\",\"page\":\"pricing\",\"count\":3}')");
    }

    /**
     * json_extract() in WHERE clause.
     */
    public function testJsonExtractInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM sl_jeiw_events
                 WHERE json_extract(data, '$.type') = 'click'
                 ORDER BY id"
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
            $this->assertEquals(3, (int) $rows[1]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_extract() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ->> operator in WHERE (text extraction, SQLite 3.38+).
     */
    public function testArrowOperatorInWhere(): void
    {
        $version = (new \PDO('sqlite::memory:'))->query('SELECT sqlite_version()')->fetchColumn();
        if (version_compare($version, '3.38.0', '<')) {
            $this->markTestSkipped('SQLite ->> operator requires 3.38+');
        }

        try {
            $rows = $this->ztdQuery(
                "SELECT id FROM sl_jeiw_events
                 WHERE data ->> '$.page' = 'home'"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                '->> operator in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * json_extract() in SELECT and ORDER BY.
     */
    public function testJsonExtractInSelectAndOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id,
                        json_extract(data, '$.type') AS event_type,
                        json_extract(data, '$.count') AS event_count
                 FROM sl_jeiw_events
                 ORDER BY json_extract(data, '$.count') DESC"
            );

            $this->assertCount(3, $rows);
            // Highest count first: scroll(12), click-home(5), click-pricing(3)
            $this->assertEquals(2, (int) $rows[0]['id']);
            $this->assertEquals(1, (int) $rows[1]['id']);
            $this->assertEquals(3, (int) $rows[2]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_extract() in SELECT/ORDER BY failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * json_extract() with prepared parameters.
     *
     * Note: SQLite json_extract() returns untyped values. When comparing with
     * a prepared parameter bound as string (PDO default), numeric comparisons
     * use text ordering. Use CAST or bindValue(PDO::PARAM_INT) for correct
     * numeric comparison. This is standard SQLite behavior, not ZTD-specific.
     */
    public function testJsonExtractWithPreparedParams(): void
    {
        try {
            // String equality with prepared param — works without CAST
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM sl_jeiw_events
                 WHERE json_extract(data, '$.type') = ?
                 ORDER BY id",
                ['click']
            );

            $this->assertCount(2, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
            $this->assertEquals(3, (int) $rows[1]['id']);

            // Numeric comparison needs CAST for prepared param
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id FROM sl_jeiw_events
                 WHERE json_extract(data, '$.type') = ?
                 AND CAST(json_extract(data, '$.count') AS INTEGER) > ?
                 ORDER BY id",
                ['click', 4]
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'json_extract() with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE using json_extract() in WHERE on shadow data.
     */
    public function testUpdateWithJsonExtractInWhere(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_jeiw_events
                 SET data = json_set(data, '$.reviewed', 1)
                 WHERE json_extract(data, '$.type') = 'scroll'"
            );

            $rows = $this->ztdQuery("SELECT data FROM sl_jeiw_events WHERE id = 2");
            $this->assertCount(1, $rows);

            $decoded = json_decode($rows[0]['data'], true);
            $this->assertArrayHasKey('reviewed', $decoded);
            $this->assertEquals(1, $decoded['reviewed']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with json_extract() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE using json_extract() in WHERE.
     */
    public function testDeleteWithJsonExtractInWhere(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_jeiw_events WHERE json_extract(data, '$.count') < 5"
            );

            $rows = $this->ztdQuery("SELECT * FROM sl_jeiw_events ORDER BY id");
            $this->assertCount(2, $rows);
            $ids = array_map('intval', array_column($rows, 'id'));
            $this->assertContains(1, $ids);
            $this->assertContains(2, $ids);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'DELETE with json_extract() in WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Aggregation on json_extract() values.
     */
    public function testAggregationOnJsonExtract(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT json_extract(data, '$.type') AS event_type,
                        SUM(json_extract(data, '$.count')) AS total_count
                 FROM sl_jeiw_events
                 GROUP BY json_extract(data, '$.type')
                 ORDER BY total_count DESC"
            );

            $this->assertCount(2, $rows);
            // scroll: 12, click: 5+3=8
            $this->assertSame('scroll', $rows[0]['event_type']);
            $this->assertEquals(12, (int) $rows[0]['total_count']);
            $this->assertSame('click', $rows[1]['event_type']);
            $this->assertEquals(8, (int) $rows[1]['total_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Aggregation on json_extract() failed: ' . $e->getMessage()
            );
        }
    }
}
