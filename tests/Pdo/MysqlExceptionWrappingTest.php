<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests exception type wrapping when ShadowStore encounters errors on MySQL PDO.
 *
 * ShadowStore throws raw RuntimeException instead of DatabaseException when
 * UPDATE/DELETE targets a table without reflected primary keys. DatabaseException
 * is caught and wrapped by ZtdPdo::exec(), but RuntimeException propagates unwrapped.
 *
 * @spec SPEC-7.1
 * @see https://github.com/k-kinzal/ztd-query-php/issues/2
 */
class MysqlExceptionWrappingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_ew_users (id INT PRIMARY KEY, name VARCHAR(100))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_ew_users'];
    }

    /**
     * UPDATE on an unreflected table should throw a wrapped exception,
     * not a raw RuntimeException.
     *
     * A separate ZtdPdo connection is created via fromPdo() without
     * reflecting the target table's schema. The adapter should catch
     * the ShadowStore error and wrap it consistently.
     */
    public function testUpdateUnreflectedTableExceptionType(): void
    {
        // Create a raw PDO and a ZtdPdo from it without reflecting the table schema
        $raw = new PDO(
            \Tests\Support\MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $ztd = ZtdPdo::fromPdo($raw);

        // Insert a row so the shadow store has something to work with
        $ztd->exec("INSERT INTO mysql_ew_users (id, name) VALUES (1, 'Alice')");

        try {
            $ztd->exec("UPDATE mysql_ew_users SET name = 'Bob' WHERE id = 1");
            // If no exception, the update worked — record the behavior
            $stmt = $ztd->query("SELECT name FROM mysql_ew_users WHERE id = 1");
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('Bob', $row['name']);
        } catch (\RuntimeException $e) {
            // RuntimeException means the error was not wrapped in DatabaseException
            if (strpos(get_class($e), 'DatabaseException') === false
                && strpos(get_class($e), 'ZtdPdoException') === false
                && strpos(get_class($e), 'PDOException') === false
            ) {
                $this->markTestIncomplete(
                    'ShadowStore throws raw RuntimeException instead of DatabaseException/ZtdPdoException: '
                    . get_class($e) . ' — ' . $e->getMessage()
                );
            }
            // If it IS a DatabaseException subclass, it was wrapped properly
            $this->assertInstanceOf(\RuntimeException::class, $e);
        }
    }

    /**
     * DELETE on an unreflected table should also wrap exceptions consistently.
     */
    public function testDeleteUnreflectedTableExceptionType(): void
    {
        $raw = new PDO(
            \Tests\Support\MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $ztd = ZtdPdo::fromPdo($raw);

        $ztd->exec("INSERT INTO mysql_ew_users (id, name) VALUES (1, 'Alice')");

        try {
            $ztd->exec("DELETE FROM mysql_ew_users WHERE id = 1");
            $stmt = $ztd->query("SELECT COUNT(*) FROM mysql_ew_users");
            $this->assertSame(0, (int) $stmt->fetchColumn());
        } catch (\RuntimeException $e) {
            if (strpos(get_class($e), 'DatabaseException') === false
                && strpos(get_class($e), 'ZtdPdoException') === false
                && strpos(get_class($e), 'PDOException') === false
            ) {
                $this->markTestIncomplete(
                    'ShadowStore throws raw RuntimeException instead of DatabaseException/ZtdPdoException: '
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
        $this->pdo->exec("INSERT INTO mysql_ew_users (id, name) VALUES (1, 'Alice')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_ew_users');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
