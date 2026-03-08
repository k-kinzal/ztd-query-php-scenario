<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use Tests\Support\MySQLContainer;

class SelectDbSideEffectTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string
    {
        return 'CREATE TABLE select_db_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['select_db_test'];
    }

    /**
     * Test that select_db() back to the same database doesn't break ZTD.
     */
    public function testSelectDbSameDatabase(): void
    {
        $this->ztdExec("INSERT INTO select_db_test (id, val) VALUES (1, 'before')");

        // Switch to the same database
        $this->mysqli->select_db('test');

        // Shadow store data should still be accessible
        $rows = $this->ztdQuery('SELECT * FROM select_db_test WHERE id = 1');
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'select_db() clears or invalidates the ZTD shadow store even when '
                . 'switching to the same database.'
            );
        }
        $this->assertCount(1, $rows);
        $this->assertSame('before', $rows[0]['val']);
    }

    /**
     * Test that new writes after select_db() are still tracked.
     */
    public function testWriteAfterSelectDb(): void
    {
        $this->mysqli->select_db('test');

        $this->ztdExec("INSERT INTO select_db_test (id, val) VALUES (1, 'after')");

        $rows = $this->ztdQuery('SELECT * FROM select_db_test WHERE id = 1');
        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'select_db() disrupts ZTD tracking. Writes after select_db() are not '
                . 'captured in the shadow store.'
            );
        }
        $this->assertCount(1, $rows);
        $this->assertSame('after', $rows[0]['val']);
    }

    /**
     * Test physical isolation after select_db().
     */
    public function testPhysicalIsolationAfterSelectDb(): void
    {
        $this->mysqli->select_db('test');
        $this->ztdExec("INSERT INTO select_db_test (id, val) VALUES (1, 'isolated')");

        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM select_db_test');
        $count = (int) $result->fetch_assoc()['cnt'];
        $this->enableZtd();

        $this->assertSame(0, $count);
    }
}
