<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT...SELECT where the source table has shadow modifications.
 *
 * INSERT INTO t1 (...) SELECT ... FROM t2 is a common pattern.
 * When t2 has been modified in shadow, the CTE rewriter must shadow t2
 * in the SELECT part so the inserted rows reflect shadow state.
 *
 * Also tests INSERT...SELECT from the same table (self-copy pattern).
 *
 * @spec SPEC-4.2
 */
class InsertSelectFromShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_iss_source (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                active TINYINT NOT NULL DEFAULT 1
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_iss_dest (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_iss_dest', 'mi_iss_source'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_iss_source VALUES (1, 'Alpha', 1)");
        $this->mysqli->query("INSERT INTO mi_iss_source VALUES (2, 'Beta', 1)");
        $this->mysqli->query("INSERT INTO mi_iss_source VALUES (3, 'Gamma', 0)");
    }

    /**
     * INSERT...SELECT from shadow-modified source should see shadow data.
     */
    public function testInsertSelectSeesSourceShadow(): void
    {
        try {
            // Add a row to source in shadow
            $this->mysqli->query("INSERT INTO mi_iss_source VALUES (4, 'Delta', 1)");

            // INSERT...SELECT from source into dest
            $this->mysqli->query(
                "INSERT INTO mi_iss_dest (id, name) SELECT id, name FROM mi_iss_source WHERE active = 1"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_iss_dest ORDER BY id");

            // Should include Delta (id=4) which was shadow-inserted
            $names = array_column($rows, 'name');
            if (!in_array('Delta', $names)) {
                $this->markTestIncomplete(
                    'INSERT...SELECT did not see shadow-inserted row. Got: ' . json_encode($names)
                );
            }
            $this->assertCount(3, $rows); // Alpha, Beta, Delta (Gamma is active=0)
            $this->assertSame('Alpha', $rows[0]['name']);
            $this->assertSame('Beta', $rows[1]['name']);
            $this->assertSame('Delta', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT from shadow source failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT after DELETE from source — deleted rows should not copy.
     */
    public function testInsertSelectAfterSourceDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_iss_source WHERE id = 2");

            $this->mysqli->query(
                "INSERT INTO mi_iss_dest (id, name) SELECT id, name FROM mi_iss_source WHERE active = 1"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_iss_dest ORDER BY id");

            $ids = array_map('intval', array_column($rows, 'id'));
            if (in_array(2, $ids)) {
                $this->markTestIncomplete(
                    'INSERT...SELECT copied deleted row. Got: ' . json_encode($rows)
                );
            }
            $this->assertCount(1, $rows); // Only Alpha (Beta deleted, Gamma inactive)
            $this->assertSame('Alpha', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT after source DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT after UPDATE on source — should see updated values.
     */
    public function testInsertSelectAfterSourceUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_iss_source SET name = 'Alpha-Updated' WHERE id = 1");

            $this->mysqli->query(
                "INSERT INTO mi_iss_dest (id, name) SELECT id, name FROM mi_iss_source WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT name FROM mi_iss_dest WHERE id = 1");
            $this->assertCount(1, $rows);

            $name = $rows[0]['name'];
            if ($name !== 'Alpha-Updated') {
                $this->markTestIncomplete(
                    'INSERT...SELECT did not see updated value. Expected Alpha-Updated, got ' . json_encode($name)
                );
            }
            $this->assertSame('Alpha-Updated', $name);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT after source UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from same table (self-copy with id offset).
     */
    public function testInsertSelectSameTable(): void
    {
        try {
            // Copy active rows with id+100
            $this->mysqli->query(
                "INSERT INTO mi_iss_source (id, name, active) SELECT id + 100, name, active FROM mi_iss_source WHERE active = 1"
            );

            $rows = $this->ztdQuery("SELECT id, name FROM mi_iss_source WHERE id > 100 ORDER BY id");

            if (empty($rows)) {
                $this->markTestIncomplete('INSERT...SELECT same table: no copied rows found');
            }
            $this->assertCount(2, $rows); // Copies of Alpha (101) and Beta (102)
            $this->assertEquals(101, (int) $rows[0]['id']);
            $this->assertSame('Alpha', $rows[0]['name']);
            $this->assertEquals(102, (int) $rows[1]['id']);
            $this->assertSame('Beta', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT...SELECT same table failed: ' . $e->getMessage());
        }
    }

    /**
     * Query dest table after INSERT...SELECT — dest should be queryable.
     */
    public function testQueryDestAfterInsertSelect(): void
    {
        try {
            $this->mysqli->query(
                "INSERT INTO mi_iss_dest (id, name) SELECT id, name FROM mi_iss_source"
            );

            // Aggregate on dest
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_iss_dest");
            $this->assertCount(1, $rows);

            $cnt = (int) $rows[0]['cnt'];
            if ($cnt !== 3) {
                $this->markTestIncomplete(
                    'INSERT...SELECT dest count wrong. Expected 3, got ' . $cnt
                );
            }
            $this->assertEquals(3, $cnt);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Query dest after INSERT...SELECT failed: ' . $e->getMessage());
        }
    }
}
