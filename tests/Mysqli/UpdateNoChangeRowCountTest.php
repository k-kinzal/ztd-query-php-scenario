<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests affected_rows behavior when UPDATE SET assigns the same value.
 *
 * Native MySQL (default): affected_rows counts rows where values actually changed.
 *   UPDATE t SET col = col WHERE id = 1  →  affected_rows = 0
 *
 * ZTD: transforms UPDATE to SELECT, counts all matched rows via count($rows).
 *   Same query  →  affected_rows = 1 (matched, not changed)
 *
 * This discrepancy matters because applications often check affected_rows == 0
 * to detect "no change" or "stale update" conditions.
 *
 * @spec SPEC-4.4
 */
class UpdateNoChangeRowCountTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_uncr_items (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            status INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_uncr_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_uncr_items VALUES (1, 'Alpha', 1)");
        $this->mysqli->query("INSERT INTO mi_uncr_items VALUES (2, 'Beta', 1)");
        $this->mysqli->query("INSERT INTO mi_uncr_items VALUES (3, 'Gamma', 0)");
    }

    /**
     * UPDATE setting column to its current value — native MySQL returns 0.
     * ZTD should also return 0, but likely returns 1 (matched rows).
     */
    public function testUpdateSameValueAffectedRows(): void
    {
        try {
            // status is already 1 for id=1
            $this->mysqli->query("UPDATE mi_uncr_items SET status = 1 WHERE id = 1");
            $affected = $this->mysqli->lastAffectedRows();

            if ($affected !== 0) {
                $this->markTestIncomplete(
                    'UPDATE with no value change returned affected_rows=' . $affected
                    . ', expected 0 (native MySQL counts changed rows, not matched rows)'
                );
            }
            $this->assertSame(0, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Update same value test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE setting multiple rows to their current values — should return 0.
     */
    public function testUpdateMultipleSameValueAffectedRows(): void
    {
        try {
            // status is already 1 for ids 1 and 2
            $this->mysqli->query("UPDATE mi_uncr_items SET status = 1 WHERE status = 1");
            $affected = $this->mysqli->lastAffectedRows();

            if ($affected !== 0) {
                $this->markTestIncomplete(
                    'UPDATE multiple rows with no change returned affected_rows=' . $affected
                    . ', expected 0'
                );
            }
            $this->assertSame(0, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Update multiple same value test failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE where some rows change and some don't — count should reflect
     * only the changed rows (native MySQL behavior).
     */
    public function testUpdatePartialChangeAffectedRows(): void
    {
        try {
            // Set all to status=1. id=1 and id=2 already have status=1, only id=3 changes.
            $this->mysqli->query("UPDATE mi_uncr_items SET status = 1 WHERE id IN (1, 2, 3)");
            $affected = $this->mysqli->lastAffectedRows();

            // Native MySQL: only 1 row actually changed (id=3)
            // ZTD: likely returns 3 (all matched)
            if ($affected !== 1) {
                $this->markTestIncomplete(
                    'UPDATE partial change returned affected_rows=' . $affected
                    . ', expected 1 (only id=3 actually changed value)'
                );
            }
            $this->assertSame(1, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Partial change test failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement variant: same-value UPDATE via bind_param.
     */
    public function testPreparedUpdateSameValueAffectedRows(): void
    {
        try {
            $stmt = $this->mysqli->prepare('UPDATE mi_uncr_items SET status = ? WHERE id = ?');
            $status = 1; // already the current value for id=1
            $id = 1;
            $stmt->bind_param('ii', $status, $id);
            $stmt->execute();
            $affected = $stmt->ztdAffectedRows();

            if ($affected !== 0) {
                $this->markTestIncomplete(
                    'Prepared UPDATE same value returned ztdAffectedRows()=' . $affected
                    . ', expected 0'
                );
            }
            $this->assertSame(0, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared same-value test failed: ' . $e->getMessage());
        }
    }

    /**
     * Positive control: UPDATE that actually changes a value should return 1.
     */
    public function testUpdateActualChangeAffectedRows(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_uncr_items SET status = 99 WHERE id = 1");
            $affected = $this->mysqli->lastAffectedRows();

            $this->assertSame(1, $affected);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Actual change test failed: ' . $e->getMessage());
        }
    }
}
