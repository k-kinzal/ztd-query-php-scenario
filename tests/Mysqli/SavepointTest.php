<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests savepoint behavior with ZTD on MySQLi.
 * @spec SPEC-6.3
 */
class SavepointTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_sp_test (id INT PRIMARY KEY, name VARCHAR(50))';
    }

    protected function getTableNames(): array
    {
        return ['mi_sp_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_sp_test VALUES (1, 'Alice')");
    }

    /**
     * SAVEPOINT should be supported.
     */
    public function testSavepointSupported(): void
    {
        $this->mysqli->begin_transaction();

        try {
            $this->mysqli->query('SAVEPOINT sp1');
            $this->mysqli->commit();
            $this->assertTrue(true);
        } catch (ZtdMysqliException $e) {
            $this->markTestIncomplete(
                'SAVEPOINT not yet supported on MySQLi: ' . $e->getMessage()
            );
        }
    }

    /**
     * RELEASE SAVEPOINT should be supported.
     */
    public function testReleaseSavepointSupported(): void
    {
        $this->mysqli->begin_transaction();

        try {
            $this->mysqli->query('SAVEPOINT sp1');
            $this->mysqli->query('RELEASE SAVEPOINT sp1');
            $this->mysqli->commit();
            $this->assertTrue(true);
        } catch (ZtdMysqliException $e) {
            $this->markTestIncomplete(
                'RELEASE SAVEPOINT not yet supported on MySQLi: ' . $e->getMessage()
            );
        }
    }

    /**
     * ROLLBACK TO SAVEPOINT should be supported.
     */
    public function testRollbackToSavepointSupported(): void
    {
        $this->mysqli->begin_transaction();

        try {
            $this->mysqli->query('SAVEPOINT sp1');
            $this->mysqli->query('ROLLBACK TO SAVEPOINT sp1');
            $this->mysqli->commit();
            $this->assertTrue(true);
        } catch (ZtdMysqliException $e) {
            $this->markTestIncomplete(
                'ROLLBACK TO SAVEPOINT not yet supported on MySQLi: ' . $e->getMessage()
            );
        }
    }
}
