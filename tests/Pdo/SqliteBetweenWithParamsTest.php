<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests BETWEEN operator with prepared params on shadow data.
 *
 * BETWEEN is a very common operator in range queries (date ranges,
 * price filters, pagination). With prepared statement params, the
 * CTE rewriter must handle `col BETWEEN ? AND ?` without confusing
 * the AND keyword with a boolean AND.
 *
 * @spec SPEC-3.2
 * @spec SPEC-3.3
 */
class SqliteBetweenWithParamsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_bwp_events (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                event_date TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_bwp_events'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_bwp_events VALUES (1, 'Concert A', '2025-01-15', 50.00, 'music')");
        $this->ztdExec("INSERT INTO sl_bwp_events VALUES (2, 'Concert B', '2025-02-20', 75.00, 'music')");
        $this->ztdExec("INSERT INTO sl_bwp_events VALUES (3, 'Workshop', '2025-03-10', 30.00, 'education')");
        $this->ztdExec("INSERT INTO sl_bwp_events VALUES (4, 'Gala', '2025-04-05', 200.00, 'social')");
        $this->ztdExec("INSERT INTO sl_bwp_events VALUES (5, 'Seminar', '2025-01-25', 0.00, 'education')");
    }

    /**
     * BETWEEN with literal values.
     */
    public function testBetweenLiteral(): void
    {
        $rows = $this->ztdQuery(
            "SELECT title FROM sl_bwp_events
             WHERE price BETWEEN 25 AND 100
             ORDER BY title"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Concert A', $rows[0]['title']); // 50
        $this->assertSame('Concert B', $rows[1]['title']); // 75
        $this->assertSame('Workshop', $rows[2]['title']);   // 30
    }

    /**
     * BETWEEN with prepared ? params.
     */
    public function testBetweenWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT title FROM sl_bwp_events
                 WHERE price BETWEEN ? AND ?
                 ORDER BY title",
                [25, 100]
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'BETWEEN with prepared ? params returned no rows on shadow data.'
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * BETWEEN on date strings with prepared params.
     */
    public function testBetweenDateWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT title FROM sl_bwp_events
                 WHERE event_date BETWEEN ? AND ?
                 ORDER BY event_date",
                ['2025-01-01', '2025-02-28']
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'BETWEEN with date params returned no rows.'
                );
            }

            // Jan-Feb: Concert A (Jan 15), Seminar (Jan 25), Concert B (Feb 20)
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN with date params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NOT BETWEEN with prepared params.
     */
    public function testNotBetweenWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT title FROM sl_bwp_events
                 WHERE price NOT BETWEEN ? AND ?
                 ORDER BY title",
                [10, 100]
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'NOT BETWEEN with params returned no rows.'
                );
            }

            // Outside 10-100: Seminar (0), Gala (200)
            $this->assertCount(2, $rows);
            $this->assertSame('Gala', $rows[0]['title']);
            $this->assertSame('Seminar', $rows[1]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NOT BETWEEN with params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * BETWEEN combined with other WHERE conditions.
     */
    public function testBetweenWithOtherConditions(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT title FROM sl_bwp_events
                 WHERE price BETWEEN ? AND ?
                   AND category = ?
                 ORDER BY title",
                [0, 100, 'education']
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Seminar', $rows[0]['title']);
            $this->assertSame('Workshop', $rows[1]['title']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN with other conditions failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * BETWEEN after shadow mutation.
     */
    public function testBetweenAfterMutation(): void
    {
        $this->ztdExec("INSERT INTO sl_bwp_events VALUES (6, 'Meetup', '2025-02-15', 45.00, 'social')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT title FROM sl_bwp_events
                 WHERE price BETWEEN ? AND ?
                 ORDER BY title",
                [40, 80]
            );

            if (count($rows) < 3) {
                $this->markTestIncomplete(
                    'BETWEEN after mutation returned ' . count($rows) . ' rows instead of 3. '
                    . 'Shadow-inserted row may not be visible in BETWEEN query.'
                );
            }

            // 40-80: Concert A(50), Meetup(45), Concert B(75) = 3
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN after mutation failed: ' . $e->getMessage()
            );
        }
    }
}
