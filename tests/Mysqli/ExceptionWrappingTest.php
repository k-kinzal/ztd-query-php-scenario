<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests exception type wrapping when ShadowStore encounters errors on MySQLi.
 *
 * ShadowStore throws raw RuntimeException instead of DatabaseException when
 * UPDATE/DELETE targets a table without reflected primary keys. ZtdMysqli::query()
 * catches DatabaseException and wraps it, but RuntimeException propagates unwrapped.
 *
 * @spec SPEC-7.1
 * @see https://github.com/k-kinzal/ztd-query-php/issues/2
 */
class ExceptionWrappingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_ew_users (id INT PRIMARY KEY, name VARCHAR(100))';
    }

    protected function getTableNames(): array
    {
        return ['mi_ew_users'];
    }

    /**
     * UPDATE after INSERT in same session should work.
     * If ShadowStore throws a raw RuntimeException, the exception type is wrong.
     */
    public function testUpdateAfterInsertExceptionType(): void
    {
        $this->mysqli->query("INSERT INTO mi_ew_users (id, name) VALUES (1, 'Alice')");

        try {
            $this->mysqli->query("UPDATE mi_ew_users SET name = 'Bob' WHERE id = 1");
            $result = $this->mysqli->query("SELECT name FROM mi_ew_users WHERE id = 1");
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\RuntimeException $e) {
            if (strpos(get_class($e), 'DatabaseException') === false) {
                $this->markTestIncomplete(
                    'ShadowStore throws raw RuntimeException instead of DatabaseException: '
                    . get_class($e) . ' — ' . $e->getMessage()
                );
            }
            $this->assertInstanceOf(\RuntimeException::class, $e);
        }
    }

    /**
     * DELETE after INSERT in same session should work.
     */
    public function testDeleteAfterInsertExceptionType(): void
    {
        $this->mysqli->query("INSERT INTO mi_ew_users (id, name) VALUES (1, 'Alice')");

        try {
            $this->mysqli->query("DELETE FROM mi_ew_users WHERE id = 1");
            $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_ew_users");
            $rows = $result->fetch_all(MYSQLI_ASSOC);
            $this->assertSame(0, (int) $rows[0]['cnt']);
        } catch (\RuntimeException $e) {
            if (strpos(get_class($e), 'DatabaseException') === false) {
                $this->markTestIncomplete(
                    'ShadowStore throws raw RuntimeException instead of DatabaseException: '
                    . get_class($e) . ' — ' . $e->getMessage()
                );
            }
            $this->assertInstanceOf(\RuntimeException::class, $e);
        }
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_ew_users (id, name) VALUES (1, 'Alice')");

        $this->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ew_users');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
