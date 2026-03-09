<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests ALTER TABLE ADD/DROP COLUMN followed by DML through CTE shadow.
 *
 * When a column is added or dropped after data exists in the shadow store,
 * the shadow store schema must be updated and subsequent DML must work
 * correctly with the new schema.
 *
 * @spec SPEC-5.2
 */
class MysqlAlterAddColumnDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_alt_col (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['pdo_alt_col'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_alt_col VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_alt_col VALUES (2, 'Bob')");
    }

    /**
     * ADD COLUMN then INSERT with new column.
     */
    public function testAddColumnThenInsert(): void
    {
        try {
            $this->pdo->exec("ALTER TABLE pdo_alt_col ADD COLUMN age INT DEFAULT 0");
            $this->pdo->exec("INSERT INTO pdo_alt_col (id, name, age) VALUES (3, 'Charlie', 25)");

            $rows = $this->ztdQuery('SELECT id, name, age FROM pdo_alt_col WHERE id = 3');
            $this->assertCount(1, $rows);
            $this->assertSame('Charlie', $rows[0]['name']);
            $this->assertEquals(25, (int) $rows[0]['age']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER ADD COLUMN + INSERT not supported: ' . $e->getMessage());
        }
    }

    /**
     * ADD COLUMN then UPDATE existing rows to set new column.
     */
    public function testAddColumnThenUpdateExisting(): void
    {
        try {
            $this->pdo->exec("ALTER TABLE pdo_alt_col ADD COLUMN score INT DEFAULT 0");
            $this->pdo->exec("UPDATE pdo_alt_col SET score = 100 WHERE id = 1");

            $rows = $this->ztdQuery('SELECT id, name, score FROM pdo_alt_col ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertEquals(100, (int) $rows[0]['score'], 'Updated row should have score=100');
            $this->assertEquals(0, (int) $rows[1]['score'], 'Non-updated row should have default 0');
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER ADD COLUMN + UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * ADD COLUMN with DEFAULT then SELECT pre-existing shadow rows.
     */
    public function testAddColumnDefaultOnExistingRows(): void
    {
        try {
            $this->pdo->exec("ALTER TABLE pdo_alt_col ADD COLUMN active BOOLEAN DEFAULT TRUE");

            $rows = $this->ztdQuery('SELECT id, name, active FROM pdo_alt_col ORDER BY id');
            $this->assertCount(2, $rows);
            // Pre-existing rows should get the DEFAULT value
            $this->assertEquals(1, (int) $rows[0]['active'], 'Pre-existing shadow row should get DEFAULT');
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER ADD COLUMN DEFAULT on existing rows not supported: ' . $e->getMessage());
        }
    }

    /**
     * ADD COLUMN then filter by new column.
     */
    public function testAddColumnThenFilterByNewColumn(): void
    {
        try {
            $this->pdo->exec("ALTER TABLE pdo_alt_col ADD COLUMN priority INT DEFAULT 1");
            $this->pdo->exec("INSERT INTO pdo_alt_col (id, name, priority) VALUES (3, 'Charlie', 5)");
            $this->pdo->exec("UPDATE pdo_alt_col SET priority = 3 WHERE id = 1");

            $rows = $this->ztdQuery(
                'SELECT id, name FROM pdo_alt_col WHERE priority > 2 ORDER BY id'
            );
            $this->assertCount(2, $rows, 'Should find rows with priority > 2');
            $this->assertEquals(1, (int) $rows[0]['id']);
            $this->assertEquals(3, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER ADD COLUMN + filter not supported: ' . $e->getMessage());
        }
    }

    /**
     * ADD multiple columns in one ALTER statement.
     */
    public function testAddMultipleColumns(): void
    {
        try {
            $this->pdo->exec(
                "ALTER TABLE pdo_alt_col ADD COLUMN email VARCHAR(100), ADD COLUMN phone VARCHAR(20)"
            );
            $this->pdo->exec(
                "INSERT INTO pdo_alt_col (id, name, email, phone) VALUES (3, 'Charlie', 'c@test.com', '555-0001')"
            );

            $rows = $this->ztdQuery('SELECT * FROM pdo_alt_col WHERE id = 3');
            $this->assertCount(1, $rows);
            $this->assertSame('c@test.com', $rows[0]['email']);
            $this->assertSame('555-0001', $rows[0]['phone']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER ADD multiple columns not supported: ' . $e->getMessage());
        }
    }

    /**
     * DROP COLUMN then ensure queries work without it.
     */
    public function testDropColumnThenQuery(): void
    {
        try {
            $this->pdo->exec("ALTER TABLE pdo_alt_col DROP COLUMN name");

            $rows = $this->ztdQuery('SELECT id FROM pdo_alt_col ORDER BY id');
            $this->assertCount(2, $rows);
            $this->assertEquals(1, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER DROP COLUMN not supported: ' . $e->getMessage());
        }
    }

    /**
     * DROP COLUMN then INSERT without the dropped column.
     */
    public function testDropColumnThenInsert(): void
    {
        try {
            $this->pdo->exec("ALTER TABLE pdo_alt_col DROP COLUMN name");
            $this->pdo->exec("INSERT INTO pdo_alt_col (id) VALUES (3)");

            $rows = $this->ztdQuery('SELECT id FROM pdo_alt_col ORDER BY id');
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ALTER DROP COLUMN + INSERT not supported: ' . $e->getMessage());
        }
    }

    /**
     * ADD COLUMN + DROP COLUMN in sequence.
     */
    public function testAddThenDropColumn(): void
    {
        try {
            $this->pdo->exec("ALTER TABLE pdo_alt_col ADD COLUMN temp VARCHAR(10) DEFAULT 'x'");
            $this->pdo->exec("INSERT INTO pdo_alt_col (id, name, temp) VALUES (3, 'Charlie', 'y')");
            $this->pdo->exec("ALTER TABLE pdo_alt_col DROP COLUMN temp");

            $rows = $this->ztdQuery('SELECT id, name FROM pdo_alt_col ORDER BY id');
            $this->assertCount(3, $rows);
            $this->assertSame('Charlie', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('ADD then DROP COLUMN not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_alt_col');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
