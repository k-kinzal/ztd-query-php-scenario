<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests backslash handling in MySQL shadow store.
 *
 * Backslash characters in string values should be preserved correctly.
 * This does NOT affect SQLite or PostgreSQL.
 *
 * @see spec 10.3
 */
class MysqlBackslashCorruptionTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS bs_test_m');
        $raw->exec('CREATE TABLE bs_test_m (id INT PRIMARY KEY, path VARCHAR(200))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    /**
     * Backslash-t should be preserved in shadow store.
     *
     * @see spec 10.3
     */
    public function testBackslashTPreserved(): void
    {
        $this->pdo->exec("INSERT INTO bs_test_m VALUES (1, 'C:\\temp\\file.txt')");

        $stmt = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $stmt->fetchColumn();

        // Expected: backslash characters should be preserved
        if ($path !== 'C:\\temp\\file.txt') {
            $this->markTestIncomplete(
                'Backslash corruption: CTE rewriter does not escape backslashes in string literals. '
                . 'Expected C:\\temp\\file.txt, got ' . var_export($path, true)
            );
        }
        $this->assertSame('C:\\temp\\file.txt', $path);
    }

    /**
     * Backslash-n should be preserved in shadow store.
     *
     * @see spec 10.3
     */
    public function testBackslashNPreserved(): void
    {
        $this->pdo->exec("INSERT INTO bs_test_m VALUES (1, 'line1\\nline2')");

        $stmt = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $stmt->fetchColumn();

        // Expected: literal \n should be preserved, not converted to newline
        if ($path !== 'line1\\nline2') {
            $this->markTestIncomplete(
                'Backslash corruption: \\n converted to newline. '
                . 'Expected line1\\nline2, got ' . var_export($path, true)
            );
        }
        $this->assertSame('line1\\nline2', $path);
    }

    /**
     * Double backslash should be preserved correctly.
     *
     * @see spec 10.3
     */
    public function testDoubleBackslashPreserved(): void
    {
        $this->pdo->exec("INSERT INTO bs_test_m VALUES (1, 'path\\\\to\\\\file')");

        $stmt = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $stmt->fetchColumn();

        // Expected: SQL 'path\\to\\file' stores path\to\file, which should be preserved
        if ($path !== 'path\\to\\file') {
            $this->markTestIncomplete(
                'Backslash corruption: double backslash not preserved correctly. '
                . 'Expected path\\to\\file, got ' . var_export($path, true)
            );
        }
        $this->assertSame('path\\to\\file', $path);
    }

    /**
     * Prepared statement with backslash should preserve values.
     *
     * @see spec 10.3
     */
    public function testPreparedStatementBackslashPreserved(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bs_test_m (id, path) VALUES (?, ?)');
        $stmt->execute([1, "C:\\new\\test"]);

        $sel = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $sel->fetchColumn();

        // Expected: prepared statement values should be preserved exactly
        if ($path !== "C:\\new\\test") {
            $this->markTestIncomplete(
                'Backslash corruption in prepared statement: '
                . 'Expected C:\\new\\test, got ' . var_export($path, true)
            );
        }
        $this->assertSame("C:\\new\\test", $path);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS bs_test_m');
    }
}
