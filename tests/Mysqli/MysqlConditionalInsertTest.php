<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests conditional INSERT patterns on MySQL MySQLi.
 *
 * Covers INSERT ... SELECT WHERE NOT EXISTS, INSERT ... SELECT with UNION,
 * and INSERT ... SELECT with subquery conditions.
 */
class MysqlConditionalInsertTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ci_log');
        $raw->query('DROP TABLE IF EXISTS mi_ci_items');
        $raw->query('CREATE TABLE mi_ci_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(20))');
        $raw->query('CREATE TABLE mi_ci_log (id INT AUTO_INCREMENT PRIMARY KEY, item_id INT, action VARCHAR(20))');
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

        $this->mysqli->query("INSERT INTO mi_ci_items VALUES (1, 'Widget', 'tools')");
        $this->mysqli->query("INSERT INTO mi_ci_items VALUES (2, 'Gadget', 'electronics')");
    }

    /**
     * INSERT ... SELECT WHERE NOT EXISTS (conditional insert).
     */
    public function testInsertSelectWhereNotExists(): void
    {
        // Insert only if item doesn't exist
        $this->mysqli->query(
            "INSERT INTO mi_ci_items (id, name, category)
             SELECT 3, 'Doohickey', 'tools'
             WHERE NOT EXISTS (SELECT 1 FROM mi_ci_items WHERE id = 3)"
        );

        $result = $this->mysqli->query('SELECT name FROM mi_ci_items WHERE id = 3');
        $this->assertSame('Doohickey', $result->fetch_assoc()['name']);
    }

    /**
     * INSERT ... SELECT WHERE NOT EXISTS skips duplicate.
     */
    public function testInsertSelectWhereNotExistsSkipsDuplicate(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_ci_items (id, name, category)
             SELECT 1, 'Duplicate', 'tools'
             WHERE NOT EXISTS (SELECT 1 FROM mi_ci_items WHERE id = 1)"
        );

        // Original row should be unchanged
        $result = $this->mysqli->query('SELECT name FROM mi_ci_items WHERE id = 1');
        $this->assertSame('Widget', $result->fetch_assoc()['name']);
    }

    /**
     * INSERT ... SELECT with UNION ALL — misdetected as multi-statement on MySQL.
     *
     * The MySQL CTE rewriter incorrectly parses INSERT...SELECT...UNION ALL
     * as a multi-statement query, throwing UnsupportedSqlException.
     */
    public function testInsertSelectWithUnionAllThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->expectExceptionMessageMatches('/multi-statement/i');
        $this->mysqli->query(
            "INSERT INTO mi_ci_items (id, name, category)
             SELECT 3, 'Item3', 'misc' UNION ALL
             SELECT 4, 'Item4', 'misc'"
        );
    }

    /**
     * INSERT ... SELECT with WHERE condition on source table.
     */
    public function testInsertSelectWithWhereCondition(): void
    {
        $this->mysqli->query(
            "INSERT INTO mi_ci_log (item_id, action)
             SELECT id, 'created' FROM mi_ci_items WHERE category = 'tools'"
        );

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ci_log');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_ci_items');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
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
            $raw->query('DROP TABLE IF EXISTS mi_ci_log');
            $raw->query('DROP TABLE IF EXISTS mi_ci_items');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
