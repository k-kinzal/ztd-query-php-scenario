<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT IGNORE behavior on MySQL ZTD via MySQLi adapter:
 * - Duplicate PK silently skipped
 * - Non-duplicate rows inserted
 * - Batch INSERT IGNORE with mixed duplicates
 * - Prepared INSERT IGNORE
 * - Physical isolation
 * @spec SPEC-4.2e
 */
class InsertIgnoreTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ins_ign (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_ins_ign'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ins_ign VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_ins_ign VALUES (2, 'Bob', 80)");
    }

    public function testInsertIgnoreDuplicateKeySkipped(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'AliceV2', 99)");

        $result = $this->mysqli->query('SELECT name, score FROM mi_ins_ign WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertEquals(90, $row['score']);
    }

    public function testInsertIgnoreNonDuplicateInserted(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $row = $result->fetch_assoc();
        $this->assertEquals(3, $row['cnt']);
    }

    public function testInsertIgnoreBatchMixedDuplicates(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'DupAlice', 99), (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $this->assertEquals(3, $result->fetch_assoc()['cnt']);

        // Duplicate row unchanged
        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);

        // New row inserted
        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 3');
        $this->assertSame('Charlie', $result->fetch_assoc()['name']);
    }

    public function testInsertIgnoreAllDuplicates(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'DupAlice', 99), (2, 'DupBob', 99)");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $this->assertEquals(2, $result->fetch_assoc()['cnt']);
    }

    public function testPreparedInsertIgnore(): void
    {
        $stmt = $this->mysqli->prepare('INSERT IGNORE INTO mi_ins_ign VALUES (?, ?, ?)');
        $id = 1;
        $name = 'DupAlice';
        $score = 99;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();

        // Original preserved
        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);

        // Insert non-duplicate
        $id = 3;
        $name = 'Charlie';
        $score = 70;
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name FROM mi_ins_ign WHERE id = 3');
        $this->assertSame('Charlie', $result->fetch_assoc()['name']);
    }

    public function testInsertIgnorePhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (1, 'DupAlice', 99)");
        $this->mysqli->query("INSERT IGNORE INTO mi_ins_ign VALUES (3, 'Charlie', 70)");

        // Physical table should be empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_ins_ign');
        $this->assertEquals(0, $result->fetch_assoc()['cnt']);
    }
}
