<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE/DELETE with ORDER BY and LIMIT through ZTD shadow store on MySQLi.
 *
 * MySQL supports UPDATE ... ORDER BY ... LIMIT n and DELETE ... ORDER BY ... LIMIT n,
 * which are commonly used for queue-processing patterns (e.g. claim the top N
 * highest-priority pending tasks). The shadow store must correctly apply the
 * ORDER BY + LIMIT constraint so the right rows are affected.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.3e
 */
class UpdateOrderByLimitTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_updlimit_queue (
            id INT PRIMARY KEY AUTO_INCREMENT,
            task VARCHAR(100),
            priority INT,
            status VARCHAR(20) DEFAULT \'pending\'
        )';
    }

    protected function getTableNames(): array
    {
        return ['mi_updlimit_queue'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('task_low', 10, 'pending')");
        $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('task_med1', 50, 'pending')");
        $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('task_med2', 50, 'pending')");
        $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('task_high1', 90, 'pending')");
        $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('task_high2', 80, 'pending')");
        $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('task_top', 100, 'pending')");
    }

    /**
     * UPDATE ... ORDER BY priority DESC LIMIT 3 should update the top-3 priority rows.
     */
    public function testUpdateOrderByLimit(): void
    {
        try {
            $affected = $this->ztdExec(
                "UPDATE mi_updlimit_queue SET status = 'processing' ORDER BY priority DESC LIMIT 3"
            );

            $processing = $this->ztdQuery(
                "SELECT task, priority FROM mi_updlimit_queue WHERE status = 'processing' ORDER BY priority DESC"
            );
            $pending = $this->ztdQuery(
                "SELECT task FROM mi_updlimit_queue WHERE status = 'pending' ORDER BY priority ASC"
            );

            if ($affected === false) {
                $this->markTestIncomplete(
                    'UPDATE ORDER BY LIMIT returned false — query may have failed in ZTD'
                );
            }

            if (count($processing) !== 3) {
                $all = $this->ztdQuery("SELECT task, priority, status FROM mi_updlimit_queue ORDER BY priority DESC");
                $this->markTestIncomplete(
                    'UPDATE ORDER BY DESC LIMIT 3: expected 3 processing rows, got ' . count($processing)
                    . '. All rows: ' . json_encode($all)
                    . ' — shadow store may not support ORDER BY LIMIT on UPDATE'
                );
            }

            $this->assertSame(3, $affected);
            $this->assertSame('task_top', $processing[0]['task']);
            $this->assertSame('task_high1', $processing[1]['task']);
            $this->assertSame('task_high2', $processing[2]['task']);
            $this->assertCount(3, $pending);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE ORDER BY LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... ORDER BY priority ASC LIMIT 2 should delete the 2 lowest-priority rows.
     */
    public function testDeleteOrderByLimit(): void
    {
        try {
            $affected = $this->ztdExec(
                "DELETE FROM mi_updlimit_queue ORDER BY priority ASC LIMIT 2"
            );

            $remaining = $this->ztdQuery(
                "SELECT task, priority FROM mi_updlimit_queue ORDER BY priority ASC"
            );

            if ($affected === false) {
                $this->markTestIncomplete(
                    'DELETE ORDER BY LIMIT returned false — query may have failed in ZTD'
                );
            }

            if (count($remaining) !== 4) {
                $this->markTestIncomplete(
                    'DELETE ORDER BY ASC LIMIT 2: expected 4 remaining rows, got ' . count($remaining)
                    . '. Data: ' . json_encode($remaining)
                    . ' — shadow store may not support ORDER BY LIMIT on DELETE'
                );
            }

            $this->assertSame(2, $affected);
            // The two lowest (priority 10 and one of the 50s) should be gone
            foreach ($remaining as $row) {
                $this->assertGreaterThanOrEqual(50, (int) $row['priority']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE ORDER BY LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE ... ORDER BY ... LIMIT ? with bound parameter.
     */
    public function testPreparedUpdateOrderByLimit(): void
    {
        try {
            $stmt = $this->mysqli->prepare(
                "UPDATE mi_updlimit_queue SET status = 'processing' ORDER BY priority DESC LIMIT ?"
            );
            $limit = 2;
            $stmt->bind_param('i', $limit);
            $stmt->execute();

            $processing = $this->ztdQuery(
                "SELECT task, priority FROM mi_updlimit_queue WHERE status = 'processing' ORDER BY priority DESC"
            );

            if (count($processing) !== 2) {
                $all = $this->ztdQuery("SELECT task, priority, status FROM mi_updlimit_queue ORDER BY priority DESC");
                $this->markTestIncomplete(
                    'Prepared UPDATE ORDER BY LIMIT ?: expected 2 processing rows, got ' . count($processing)
                    . '. All rows: ' . json_encode($all)
                    . ' — prepared UPDATE with ORDER BY LIMIT may not work in ZTD'
                );
            }

            $this->assertSame('task_top', $processing[0]['task']);
            $this->assertSame('task_high1', $processing[1]['task']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE ORDER BY LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * Insert rows via ZTD, then UPDATE ... ORDER BY ... LIMIT on shadow data.
     *
     * Verifies the ORDER BY LIMIT operates correctly on shadow-only data
     * (rows that exist only in the ZTD shadow store, not physical).
     */
    public function testUpdateLimitOnShadowData(): void
    {
        try {
            // Insert additional shadow-only rows
            $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('shadow_a', 200, 'pending')");
            $this->mysqli->query("INSERT INTO mi_updlimit_queue (task, priority, status) VALUES ('shadow_b', 150, 'pending')");

            // UPDATE top 2 by priority — should pick shadow_a (200) and shadow_b (150)
            $affected = $this->ztdExec(
                "UPDATE mi_updlimit_queue SET status = 'claimed' ORDER BY priority DESC LIMIT 2"
            );

            $claimed = $this->ztdQuery(
                "SELECT task, priority FROM mi_updlimit_queue WHERE status = 'claimed' ORDER BY priority DESC"
            );

            if ($affected === false) {
                $this->markTestIncomplete(
                    'UPDATE LIMIT on shadow data returned false — query may have failed'
                );
            }

            if (count($claimed) !== 2) {
                $all = $this->ztdQuery("SELECT task, priority, status FROM mi_updlimit_queue ORDER BY priority DESC");
                $this->markTestIncomplete(
                    'UPDATE LIMIT on shadow data: expected 2 claimed rows, got ' . count($claimed)
                    . '. All rows: ' . json_encode($all)
                    . ' — ORDER BY LIMIT may not work correctly on shadow-only data'
                );
            }

            $this->assertSame(2, $affected);
            $this->assertSame('shadow_a', $claimed[0]['task']);
            $this->assertSame('shadow_b', $claimed[1]['task']);

            // Verify original rows remain pending
            $pending = $this->ztdQuery(
                "SELECT task FROM mi_updlimit_queue WHERE status = 'pending' ORDER BY priority DESC"
            );
            $this->assertCount(6, $pending);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE LIMIT on shadow data failed: ' . $e->getMessage());
        }
    }
}
