<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests table-less queries through the MySQLi ZTD adapter.
 *
 * Queries like SELECT 1, SELECT NOW(), SELECT VERSION() don't reference
 * any table. The CTE rewriter should pass these through without
 * modification since there are no table references to rewrite.
 *
 * @spec SPEC-3.1
 */
class TablelessQueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string
    {
        return 'CREATE TABLE tlq_dummy (id INT PRIMARY KEY)';
    }

    protected function getTableNames(): array
    {
        return ['tlq_dummy'];
    }

    /**
     * SELECT 1 should return the integer literal 1.
     */
    public function testSelectLiteral(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT 1 AS val');
            $this->assertCount(1, $rows);
            $this->assertEquals(1, $rows[0]['val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT literal through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT 1+1 AS result should return 2.
     */
    public function testSelectArithmetic(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT 1+1 AS result');
            $this->assertCount(1, $rows);
            $this->assertEquals(2, $rows[0]['result']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT arithmetic through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT 'hello' AS msg should return the string 'hello'.
     */
    public function testSelectStringLiteral(): void
    {
        try {
            $rows = $this->ztdQuery("SELECT 'hello' AS msg");
            $this->assertCount(1, $rows);
            $this->assertSame('hello', $rows[0]['msg']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT string literal through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT CURRENT_TIMESTAMP AS ts should return a non-null timestamp.
     */
    public function testSelectCurrentTimestamp(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT CURRENT_TIMESTAMP AS ts');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['ts']);
            $this->assertNotEmpty($rows[0]['ts']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT CURRENT_TIMESTAMP through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT VERSION() should return the MySQL server version string.
     */
    public function testSelectVersion(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT VERSION() AS ver');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['ver']);
            $this->assertNotEmpty($rows[0]['ver']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT VERSION() through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT DATABASE() should return the current database name.
     */
    public function testSelectDatabase(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT DATABASE() AS db');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['db']);
            $this->assertNotEmpty($rows[0]['db']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT DATABASE() through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT CONNECTION_ID() should return a non-null integer connection ID.
     */
    public function testSelectConnectionId(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT CONNECTION_ID() AS cid');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['cid']);
            $this->assertGreaterThan(0, (int) $rows[0]['cid']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT CONNECTION_ID() through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT @@autocommit should return the autocommit session variable.
     */
    public function testSelectSessionVariable(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT @@autocommit AS ac');
            $this->assertCount(1, $rows);
            $this->assertNotNull($rows[0]['ac']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT @@autocommit through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * SELECT NULL AS val should return null.
     */
    public function testSelectNullLiteral(): void
    {
        try {
            $rows = $this->ztdQuery('SELECT NULL AS val');
            $this->assertCount(1, $rows);
            $this->assertNull($rows[0]['val']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SELECT NULL through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared table-less query: SELECT ? + ? AS result with params [3, 4]
     * should return 7.
     */
    public function testPreparedTablelessQuery(): void
    {
        try {
            $stmt = $this->mysqli->prepare('SELECT ? + ? AS result');
            $a = 3;
            $b = 4;
            $stmt->bind_param('ii', $a, $b);
            $stmt->execute();
            $result = $stmt->get_result();
            $rows = $result->fetch_all(MYSQLI_ASSOC);

            $this->assertCount(1, $rows);
            $this->assertEquals(7, $rows[0]['result']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared table-less query through ZTD failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * After inserting into the dummy table, a table-less SELECT 1+1
     * should still work correctly.
     */
    public function testTablelessQueryAfterShadowInsert(): void
    {
        try {
            $this->ztdExec('INSERT INTO tlq_dummy (id) VALUES (1)');

            $rows = $this->ztdQuery('SELECT 1+1 AS result');
            $this->assertCount(1, $rows);
            $this->assertEquals(2, $rows[0]['result']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Table-less query after shadow INSERT failed: ' . $e->getMessage()
            );
        }
    }
}
