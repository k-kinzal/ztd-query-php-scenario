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
 * Tests backslash corruption in MySQL shadow store (documented in spec 10.3).
 *
 * Discovery: Backslash characters in string values inserted via prepared statements
 * are corrupted in the shadow store. The CTE rewriter embeds values as string literals
 * without escaping backslashes, causing MySQL to interpret escape sequences:
 * \t → tab, \n → newline, \b → backspace, \r → carriage return, \0 → null byte, \\ → single backslash.
 * Unrecognized sequences like \f drop the backslash.
 *
 * This does NOT affect SQLite or PostgreSQL.
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
     * Backslash-t is corrupted to tab character in shadow store.
     */
    public function testBackslashTCorruptedToTab(): void
    {
        $this->pdo->exec("INSERT INTO bs_test_m VALUES (1, 'C:\\temp\\file.txt')");

        $stmt = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $stmt->fetchColumn();

        // On MySQL, \t becomes tab - path is corrupted
        $this->assertNotSame('C:\\temp\\file.txt', $path);
        $this->assertStringContainsString("\t", $path); // tab character
    }

    /**
     * Backslash-n is corrupted to newline in shadow store.
     */
    public function testBackslashNCorruptedToNewline(): void
    {
        $this->pdo->exec("INSERT INTO bs_test_m VALUES (1, 'line1\\nline2')");

        $stmt = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $stmt->fetchColumn();

        // \n becomes actual newline
        $this->assertNotSame('line1\\nline2', $path);
        $this->assertStringContainsString("\n", $path);
    }

    /**
     * Double backslash: the corruption affects even escaped backslashes.
     * In SQL: 'path\\to\\file' stores path\to\file.
     * But CTE rewriter re-embeds without escaping, so \t → tab, \f → drops backslash.
     */
    public function testDoubleBackslashAlsoCorrupted(): void
    {
        $this->pdo->exec("INSERT INTO bs_test_m VALUES (1, 'path\\\\to\\\\file')");

        $stmt = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $stmt->fetchColumn();

        // The value 'path\to\file' gets re-embedded in CTE as 'path\to\file'
        // MySQL interprets \t as tab and \f drops the backslash
        $this->assertNotSame('path\\to\\file', $path);
        $this->assertStringContainsString("\t", $path); // \t → tab
    }

    /**
     * Prepared statement with backslash also corrupted.
     */
    public function testPreparedStatementBackslashCorrupted(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO bs_test_m (id, path) VALUES (?, ?)');
        $stmt->execute([1, "C:\\new\\test"]);

        $sel = $this->pdo->query('SELECT path FROM bs_test_m WHERE id = 1');
        $path = $sel->fetchColumn();

        // \n in the middle of path becomes newline
        $this->assertStringContainsString("\n", $path);
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
