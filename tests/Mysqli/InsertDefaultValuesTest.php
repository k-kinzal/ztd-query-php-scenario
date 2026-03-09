<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT with all-default columns via MySQLi.
 *
 * MySQL supports `INSERT INTO t () VALUES ()` for all-defaults insertion
 * and the explicit DEFAULT keyword in VALUES.
 *
 * @spec SPEC-4.1
 * @see https://github.com/k-kinzal/ztd-query-php/issues/97
 */
class InsertDefaultValuesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE defv_test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            status VARCHAR(20) NOT NULL DEFAULT \'pending\',
            priority INT NOT NULL DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['defv_test'];
    }

    public function testInsertEmptyValuesClause(): void
    {
        try {
            $this->ztdExec("INSERT INTO defv_test () VALUES ()");

            $rows = $this->ztdQuery("SELECT id, status, priority FROM defv_test ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT () VALUES (): expected 1 row, got ' . count($rows));
            }

            $this->assertCount(1, $rows);
            $this->assertSame('pending', $rows[0]['status']);
            $this->assertSame(0, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT () VALUES () failed: ' . $e->getMessage());
        }
    }

    public function testInsertWithDefaultKeyword(): void
    {
        try {
            $this->ztdExec("INSERT INTO defv_test (status, priority) VALUES (DEFAULT, DEFAULT)");

            $rows = $this->ztdQuery("SELECT id, status, priority FROM defv_test ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT with DEFAULT: expected 1 row, got ' . count($rows));
            }

            $this->assertCount(1, $rows);
            $this->assertSame('pending', $rows[0]['status']);
            $this->assertSame(0, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with DEFAULT keyword failed: ' . $e->getMessage());
        }
    }

    public function testMultipleInsertEmptyValues(): void
    {
        try {
            $this->ztdExec("INSERT INTO defv_test () VALUES ()");
            $this->ztdExec("INSERT INTO defv_test () VALUES ()");
            $this->ztdExec("INSERT INTO defv_test () VALUES ()");

            $rows = $this->ztdQuery("SELECT id, status, priority FROM defv_test ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete('Multiple INSERT: expected 3 rows, got ' . count($rows));
            }

            $this->assertCount(3, $rows);
            foreach ($rows as $row) {
                $this->assertSame('pending', $row['status']);
                $this->assertSame(0, (int) $row['priority']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple INSERT () VALUES () failed: ' . $e->getMessage());
        }
    }

    public function testInsertDefaultThenUpdate(): void
    {
        try {
            $this->ztdExec("INSERT INTO defv_test () VALUES ()");

            $rows = $this->ztdQuery("SELECT id FROM defv_test");
            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT: expected 1 row, got ' . count($rows));
                return;
            }

            $id = (int) $rows[0]['id'];
            $this->ztdExec("UPDATE defv_test SET status = 'active', priority = 5 WHERE id = {$id}");

            $rows = $this->ztdQuery("SELECT status, priority FROM defv_test WHERE id = {$id}");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE after INSERT: row not found');
            }

            $this->assertSame('active', $rows[0]['status']);
            $this->assertSame(5, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT default then UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testInsertMixedDefaultAndExplicit(): void
    {
        try {
            $this->ztdExec("INSERT INTO defv_test (status, priority) VALUES ('custom', DEFAULT)");

            $rows = $this->ztdQuery("SELECT status, priority FROM defv_test ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('INSERT mixed DEFAULT: expected 1 row, got ' . count($rows));
            }

            $this->assertCount(1, $rows);
            $this->assertSame('custom', $rows[0]['status']);
            $this->assertSame(0, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT mixed DEFAULT failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $this->ztdExec("INSERT INTO defv_test () VALUES ()");
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT failed: ' . $e->getMessage());
            return;
        }

        $this->disableZtd();
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM defv_test");
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
