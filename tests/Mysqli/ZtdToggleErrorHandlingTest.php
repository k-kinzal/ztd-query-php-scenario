<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests error handling across ZTD toggle boundaries on MySQLi:
 * - Errors when ZTD is enabled vs disabled
 * - State consistency after toggle + error
 * - Prepared statements created with ZTD on/off and executed across toggles
 * - Shadow data visibility vs physical data across toggles
 */
class ZtdToggleErrorHandlingTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_zte_users');
        $raw->query('CREATE TABLE mi_zte_users (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->query("INSERT INTO mi_zte_users VALUES (1, 'Alice')");
        $raw->query("INSERT INTO mi_zte_users VALUES (2, 'Bob')");
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        // Populate shadow store with same data as physical
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (2, 'Bob')");
    }

    public function testErrorWithZtdEnabledDoesNotCorruptShadow(): void
    {
        try {
            $this->mysqli->query('SELECT * FROM nonexistent_xyz');
        } catch (\Exception $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT name FROM mi_zte_users WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testErrorWithZtdDisabledThenReEnable(): void
    {
        $this->mysqli->disableZtd();

        try {
            $this->mysqli->query('SELECT * FROM nonexistent_xyz');
        } catch (\Exception $e) {
            // Expected — MySQLi may return false instead of throwing
        }

        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT name FROM mi_zte_users WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testToggleAfterErrorPreservesState(): void
    {
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (3, 'Charlie')");

        try {
            $this->mysqli->query('INSERT INTO nonexistent_xyz VALUES (1)');
        } catch (\Exception $e) {
            // Expected
        }

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_zte_users');
        $this->assertEquals(3, $result->fetch_assoc()['cnt']);
    }

    public function testPreparedStatementSurvivesToggle(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_zte_users WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $stmt->execute();
        $result = $stmt->get_result();
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    public function testShadowInsertNotVisibleWhenDisabled(): void
    {
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (4, 'ShadowUser')");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_zte_users');
        $this->assertEquals(3, $result->fetch_assoc()['cnt']);

        // Disable ZTD — physical table only has 2 rows
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_zte_users');
        $this->assertEquals(2, $result->fetch_assoc()['cnt']);
    }

    public function testMultipleToggleCyclesAccumulateShadow(): void
    {
        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (3, 'Charlie')");
        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $this->mysqli->query("INSERT INTO mi_zte_users VALUES (4, 'Diana')");

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_zte_users');
        $this->assertEquals(4, $result->fetch_assoc()['cnt']);

        $this->mysqli->disableZtd();
        $this->mysqli->enableZtd();

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM mi_zte_users');
        $this->assertEquals(4, $result->fetch_assoc()['cnt']);
    }

    public function testPhysicalQueryAfterDisableSeesOriginalData(): void
    {
        $this->mysqli->query("UPDATE mi_zte_users SET name = 'Modified' WHERE id = 1");

        $result = $this->mysqli->query('SELECT name FROM mi_zte_users WHERE id = 1');
        $this->assertSame('Modified', $result->fetch_assoc()['name']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT name FROM mi_zte_users WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_zte_users');
            $raw->close();
        } catch (\Exception $e) {
            // Container may be unavailable during cleanup
        }
    }
}
