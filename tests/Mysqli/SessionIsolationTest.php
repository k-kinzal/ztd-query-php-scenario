<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;
use Tests\Support\MySQLContainer;

/** @spec SPEC-2.4 */
class SessionIsolationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE session_test (id INT PRIMARY KEY, val VARCHAR(255))';
    }

    protected function getTableNames(): array
    {
        return ['session_test'];
    }


    public function testShadowDataNotSharedBetweenInstances(): void
    {
        $ztd1 = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $ztd2 = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        // Insert in instance 1
        $ztd1->query("INSERT INTO session_test (id, val) VALUES (1, 'from_ztd1')");

        // Instance 1 sees the row
        $result = $ztd1->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(1, $result->num_rows);

        // Instance 2 does NOT see it
        $result = $ztd2->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(0, $result->num_rows);

        $ztd1->close();
        $ztd2->close();
    }

    public function testShadowDataNotPersistedAcrossLifecycle(): void
    {
        $ztd = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $ztd->query("INSERT INTO session_test (id, val) VALUES (1, 'temporary')");

        $result = $ztd->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(1, $result->num_rows);
        $ztd->close();

        // New instance should NOT see previous shadow data
        $ztd2 = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $result = $ztd2->query('SELECT * FROM session_test WHERE id = 1');
        $this->assertSame(0, $result->num_rows);

        $ztd2->close();
    }
}
