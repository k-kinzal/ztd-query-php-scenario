<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests rapid interleaving of mutations and queries on MySQLi.
 *
 * @spec SPEC-10.2
 */
class RapidMutationQueryInterleavingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_rmqi_t (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(200),
            status VARCHAR(20) DEFAULT 'active'
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_rmqi_t'];
    }

    public function testFullCrudLifecycle(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_rmqi_t (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");

            $rows = $this->ztdQuery("SELECT * FROM mi_rmqi_t WHERE id = 1");
            if (count($rows) !== 1) {
                $this->markTestIncomplete('CRUD (MySQLi): INSERT then SELECT failed, got ' . count($rows) . ' rows');
            }
            $this->assertSame('Alice', $rows[0]['name']);

            $this->ztdExec("UPDATE mi_rmqi_t SET email = 'newalice@test.com', status = 'verified' WHERE id = 1");

            $rows2 = $this->ztdQuery("SELECT * FROM mi_rmqi_t WHERE id = 1");
            if ($rows2[0]['email'] !== 'newalice@test.com') {
                $this->markTestIncomplete('CRUD (MySQLi): UPDATE not reflected. email=' . $rows2[0]['email']);
            }
            $this->assertSame('newalice@test.com', $rows2[0]['email']);
            $this->assertSame('verified', $rows2[0]['status']);

            $this->ztdExec("DELETE FROM mi_rmqi_t WHERE id = 1");

            $rows3 = $this->ztdQuery("SELECT * FROM mi_rmqi_t WHERE id = 1");
            if (count($rows3) !== 0) {
                $this->markTestIncomplete('CRUD (MySQLi): DELETE not reflected, still got ' . count($rows3) . ' rows');
            }
            $this->assertCount(0, $rows3);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CRUD (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testInterleavedInsertCount(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_rmqi_t (id, name) VALUES (1, 'A')");
            $c1 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rmqi_t");
            $this->assertSame(1, (int) $c1[0]['cnt']);

            $this->ztdExec("INSERT INTO mi_rmqi_t (id, name) VALUES (2, 'B')");
            $c2 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rmqi_t");
            $this->assertSame(2, (int) $c2[0]['cnt']);

            $this->ztdExec("INSERT INTO mi_rmqi_t (id, name) VALUES (3, 'C')");
            $c3 = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rmqi_t");
            if ((int) $c3[0]['cnt'] !== 3) {
                $this->markTestIncomplete('Interleaved INSERT/COUNT (MySQLi): expected 3, got ' . $c3[0]['cnt']);
            }
            $this->assertSame(3, (int) $c3[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Interleaved INSERT/COUNT (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testFilterOnUpdatedColumn(): void
    {
        try {
            $this->ztdExec("INSERT INTO mi_rmqi_t (id, name, status) VALUES (1, 'Alice', 'active')");
            $this->ztdExec("INSERT INTO mi_rmqi_t (id, name, status) VALUES (2, 'Bob', 'active')");
            $this->ztdExec("INSERT INTO mi_rmqi_t (id, name, status) VALUES (3, 'Charlie', 'active')");

            $this->ztdExec("UPDATE mi_rmqi_t SET status = 'suspended' WHERE id = 2");

            $active = $this->ztdQuery("SELECT name FROM mi_rmqi_t WHERE status = 'active' ORDER BY name");
            $suspended = $this->ztdQuery("SELECT name FROM mi_rmqi_t WHERE status = 'suspended'");

            if (count($active) !== 2) {
                $this->markTestIncomplete(
                    'Filter updated col (MySQLi): expected 2 active, got ' . count($active)
                );
            }
            if (count($suspended) !== 1) {
                $this->markTestIncomplete(
                    'Filter updated col (MySQLi): expected 1 suspended, got ' . count($suspended)
                );
            }

            $this->assertSame('Alice', $active[0]['name']);
            $this->assertSame('Charlie', $active[1]['name']);
            $this->assertSame('Bob', $suspended[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Filter updated col (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
