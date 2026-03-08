<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests TRUNCATE TABLE + re-insert workflow via MySQLi.
 *
 * Cross-platform parity with MysqlTruncateReinsertTest (PDO).
 * @spec SPEC-5.3
 */
class TruncateReinsertTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_tr_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_tr_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_tr_test VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_tr_test VALUES (2, 'Bob')");
    }

    /**
     * TRUNCATE clears shadow store.
     */
    public function testTruncateClearsShadow(): void
    {
        $this->mysqli->query('TRUNCATE TABLE mi_tr_test');

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_tr_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * INSERT after TRUNCATE works.
     */
    public function testInsertAfterTruncate(): void
    {
        $this->mysqli->query('TRUNCATE TABLE mi_tr_test');
        $this->mysqli->query("INSERT INTO mi_tr_test VALUES (1, 'Charlie')");

        $result = $this->mysqli->query('SELECT name FROM mi_tr_test WHERE id = 1');
        $this->assertSame('Charlie', $result->fetch_assoc()['name']);
    }

    /**
     * Multiple truncate-reinsert cycles.
     */
    public function testMultipleTruncateCycles(): void
    {
        $this->mysqli->query('TRUNCATE TABLE mi_tr_test');
        $this->mysqli->query("INSERT INTO mi_tr_test VALUES (1, 'Round1')");

        $this->mysqli->query('TRUNCATE TABLE mi_tr_test');
        $this->mysqli->query("INSERT INTO mi_tr_test VALUES (1, 'Round2')");

        $result = $this->mysqli->query('SELECT name FROM mi_tr_test WHERE id = 1');
        $this->assertSame('Round2', $result->fetch_assoc()['name']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('TRUNCATE TABLE mi_tr_test');
        $this->mysqli->query("INSERT INTO mi_tr_test VALUES (1, 'New')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_tr_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
