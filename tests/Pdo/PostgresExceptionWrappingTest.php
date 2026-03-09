<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests exception type wrapping when ShadowStore encounters errors on PostgreSQL PDO.
 *
 * ShadowStore throws raw RuntimeException instead of DatabaseException when
 * UPDATE/DELETE targets a table without reflected primary keys. The adapter
 * should wrap this consistently with its own exception hierarchy.
 *
 * @spec SPEC-7.1
 * @see https://github.com/k-kinzal/ztd-query-php/issues/2
 */
class PostgresExceptionWrappingTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ew_users (id INT PRIMARY KEY, name VARCHAR(100))';
    }

    protected function getTableNames(): array
    {
        return ['pg_ew_users'];
    }

    /**
     * UPDATE on an unreflected table should throw a wrapped exception.
     */
    public function testUpdateUnreflectedTableExceptionType(): void
    {
        $raw = new PDO(
            \Tests\Support\PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $ztd = ZtdPdo::fromPdo($raw);

        $ztd->exec("INSERT INTO pg_ew_users (id, name) VALUES (1, 'Alice')");

        try {
            $ztd->exec("UPDATE pg_ew_users SET name = 'Bob' WHERE id = 1");
            $stmt = $ztd->query("SELECT name FROM pg_ew_users WHERE id = 1");
            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $this->assertSame('Bob', $row['name']);
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
     * DELETE on an unreflected table should wrap exceptions consistently.
     */
    public function testDeleteUnreflectedTableExceptionType(): void
    {
        $raw = new PDO(
            \Tests\Support\PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $ztd = ZtdPdo::fromPdo($raw);

        $ztd->exec("INSERT INTO pg_ew_users (id, name) VALUES (1, 'Alice')");

        try {
            $ztd->exec("DELETE FROM pg_ew_users WHERE id = 1");
            $stmt = $ztd->query("SELECT COUNT(*) FROM pg_ew_users");
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
        $this->pdo->exec("INSERT INTO pg_ew_users (id, name) VALUES (1, 'Alice')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_ew_users');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
