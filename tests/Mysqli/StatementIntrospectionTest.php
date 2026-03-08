<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests MySQLi statement introspection properties under ZTD mode.
 * Documents which properties work and which are limited.
 * @spec SPEC-4.7, SPEC-4.12
 */
class StatementIntrospectionTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_intro (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_intro'];
    }


    public function testParamCountOnPreparedSelectThrowsAfterClose(): void
    {
        $stmt = $this->mysqli->prepare('SELECT * FROM mi_intro WHERE id = ?');
        // ZTD rewrites and immediately closes the underlying statement
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('already closed');
        $_ = $stmt->param_count;
    }

    public function testAffectedRowsPropertyNotAllowed(): void
    {
        $this->mysqli->query("INSERT INTO mi_intro VALUES (1, 'Alice', 100)");
        // affected_rows property access throws on ZtdMysqli
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Property access is not allowed yet');
        $_ = $this->mysqli->affected_rows;
    }

    public function testZtdAffectedRowsOnStatementWorks(): void
    {
        $this->mysqli->query("INSERT INTO mi_intro VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_intro VALUES (2, 'Bob', 85)");

        $stmt = $this->mysqli->prepare('UPDATE mi_intro SET score = ? WHERE score > ?');
        $newScore = 999;
        $minScore = 0;
        $stmt->bind_param('ii', $newScore, $minScore);
        $stmt->execute();
        $this->assertSame(2, $stmt->ztdAffectedRows());
    }

    public function testInsertIdPropertyNotAllowed(): void
    {
        $this->mysqli->query("INSERT INTO mi_intro VALUES (1, 'Alice', 100)");
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Property access is not allowed yet');
        $_ = $this->mysqli->insert_id;
    }

    public function testFieldCountAfterQuery(): void
    {
        $this->mysqli->query("INSERT INTO mi_intro VALUES (1, 'Alice', 100)");
        $result = $this->mysqli->query('SELECT id, name FROM mi_intro WHERE id = 1');
        $this->assertSame(2, $result->field_count);
    }

    public function testNumRowsAfterQuery(): void
    {
        $this->mysqli->query("INSERT INTO mi_intro VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_intro VALUES (2, 'Bob', 85)");
        $result = $this->mysqli->query('SELECT * FROM mi_intro');
        $this->assertSame(2, $result->num_rows);
    }

    public function testErrnoAndErrorAfterSuccess(): void
    {
        $this->mysqli->query("INSERT INTO mi_intro VALUES (1, 'Alice', 100)");
        $this->assertSame(0, $this->mysqli->errno);
        $this->assertSame('', $this->mysqli->error);
    }

    public function testPreparedStatementStoreResultReturnsTrueAfterExecute(): void
    {
        $this->mysqli->query("INSERT INTO mi_intro VALUES (1, 'Alice', 100)");

        $stmt = $this->mysqli->prepare('SELECT * FROM mi_intro WHERE score > ?');
        $min = 0;
        $stmt->bind_param('i', $min);
        $stmt->execute();
        // store_result works on ZTD prepared SELECT statements
        $result = $stmt->store_result();
        $this->assertTrue($result);
    }
}
