<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests backslash character corruption in MySQL shadow store via MySQLi.
 *
 * Cross-platform parity with MysqlBackslashCorruptionTest (PDO).
 * MySQL's CTE rewriter embeds values as string literals without escaping
 * backslashes, causing escape sequence interpretation.
 */
class BackslashCorruptionTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_bslash_test');
        $raw->query('CREATE TABLE mi_bslash_test (id INT PRIMARY KEY, path VARCHAR(200))');
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
    }

    /**
     * Backslash-t is corrupted to tab character.
     */
    public function testBackslashTCorrupted(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_bslash_test VALUES (?, ?)');
        $id = 1;
        $path = 'C:\test\temp';
        $stmt->bind_param('is', $id, $path);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT path FROM mi_bslash_test WHERE id = 1');
        $retrieved = $result->fetch_assoc()['path'];

        // MySQL CTE rewriter corrupts \t to tab, \te to tab+'e'
        // The retrieved value will NOT match the original
        $this->assertNotSame('C:\test\temp', $retrieved);
    }

    /**
     * Simple string without backslash works correctly.
     */
    public function testSimpleStringNoCorruption(): void
    {
        $stmt = $this->mysqli->prepare('INSERT INTO mi_bslash_test VALUES (?, ?)');
        $id = 1;
        $path = '/usr/local/bin';
        $stmt->bind_param('is', $id, $path);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT path FROM mi_bslash_test WHERE id = 1');
        $this->assertSame('/usr/local/bin', $result->fetch_assoc()['path']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_bslash_test VALUES (1, 'test')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_bslash_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_bslash_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
