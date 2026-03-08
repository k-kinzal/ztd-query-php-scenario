<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;
use Tests\Support\MySQLContainer;

/**
 * Tests session isolation in ZTD mode on MySQL via PDO:
 * shadow data is not shared between ZtdPdo instances.
 * @spec SPEC-2.4
 */
class MysqlSessionIsolationTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_session_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['mysql_session_test'];
    }


    public function testShadowDataNotSharedBetweenInstances(): void
    {
        $pdo1 = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $pdo2 = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo1->exec("INSERT INTO mysql_session_test (id, val) VALUES (1, 'from_pdo1')");

        $stmt = $pdo1->query('SELECT * FROM mysql_session_test WHERE id = 1');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $stmt = $pdo2->query('SELECT * FROM mysql_session_test WHERE id = 1');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testShadowDataNotPersistedAcrossLifecycle(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $pdo->exec("INSERT INTO mysql_session_test (id, val) VALUES (1, 'temporary')");

        $stmt = $pdo->query('SELECT * FROM mysql_session_test WHERE id = 1');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // New wrapper on a new connection — shadow data should be gone
        $pdo2 = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $stmt = $pdo2->query('SELECT * FROM mysql_session_test WHERE id = 1');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
