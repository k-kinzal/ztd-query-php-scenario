<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests that stmt_init() returns a raw mysqli_stmt that bypasses ZTD.
 *
 * ZtdMysqli::stmt_init() delegates directly to the inner mysqli instance
 * and returns a plain mysqli_stmt — NOT a ZtdMysqliStatement. This means
 * queries prepared via stmt_init() bypass ZTD entirely and operate on the
 * physical database.
 *
 * This is a potential user pitfall: if a user calls $mysqli->stmt_init()
 * instead of $mysqli->prepare(), they unknowingly bypass ZTD.
 */
class StmtInitBypassTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_si_test');
        $raw->query('CREATE TABLE mi_si_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        // Clean physical table before each test since stmt_init writes to physical DB
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DELETE FROM mi_si_test');
        $raw->close();

        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    /**
     * stmt_init() returns a raw mysqli_stmt, NOT a ZtdMysqliStatement.
     */
    public function testStmtInitReturnsRawStatement(): void
    {
        $stmt = $this->mysqli->stmt_init();
        $this->assertInstanceOf(\mysqli_stmt::class, $stmt);
        // It should NOT be a ZtdMysqliStatement
        $this->assertNotInstanceOf(
            \ZtdQuery\Adapter\Mysqli\ZtdMysqliStatement::class,
            $stmt
        );
    }

    /**
     * INSERT via stmt_init() writes to physical database, bypassing ZTD.
     */
    public function testStmtInitInsertWritesToPhysical(): void
    {
        // Use stmt_init() for INSERT — this bypasses ZTD
        $stmt = $this->mysqli->stmt_init();
        $stmt->prepare("INSERT INTO mi_si_test (id, name, score) VALUES (1, 'Alice', 90)");
        $stmt->execute();
        $stmt->close();

        // Verify data is in physical DB by disabling ZTD
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_si_test');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
        $this->mysqli->enableZtd();
    }

    /**
     * Data inserted via stmt_init() is NOT visible through ZTD queries.
     *
     * Because stmt_init() bypasses ZTD, the data goes to physical DB.
     * But ZTD shadow store starts empty (replaces physical table),
     * so the data is invisible through ZTD-enabled SELECT.
     */
    public function testStmtInitDataInvisibleThroughZtd(): void
    {
        // Insert via stmt_init (bypasses ZTD, writes to physical)
        $stmt = $this->mysqli->stmt_init();
        $stmt->prepare("INSERT INTO mi_si_test (id, name, score) VALUES (1, 'Alice', 90)");
        $stmt->execute();
        $stmt->close();

        // ZTD-enabled query sees empty shadow store
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_si_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * prepare() vs stmt_init(): different ZTD behavior.
     *
     * prepare() wraps in ZtdMysqliStatement (ZTD-aware).
     * stmt_init() returns raw stmt (ZTD-unaware).
     */
    public function testPrepareVsStmtInitDifference(): void
    {
        // Insert via ZTD-aware prepare()
        $this->mysqli->query("INSERT INTO mi_si_test (id, name, score) VALUES (1, 'Alice', 90)");

        // ZTD can see the shadow data
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_si_test');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        // Physical DB is still empty (ZTD isolation)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_si_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
        $this->mysqli->enableZtd();

        // Now insert via stmt_init (bypasses ZTD)
        $stmt = $this->mysqli->stmt_init();
        $stmt->prepare("INSERT INTO mi_si_test (id, name, score) VALUES (2, 'Bob', 80)");
        $stmt->execute();
        $stmt->close();

        // ZTD sees only shadow data (1 row from prepare path)
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_si_test');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);

        // Physical DB has only stmt_init data (1 row)
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_si_test');
        $this->assertSame(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * SELECT via stmt_init() reads from physical DB, not shadow.
     */
    public function testStmtInitSelectReadsPhysical(): void
    {
        // Insert shadow data via ZTD-aware path
        $this->mysqli->query("INSERT INTO mi_si_test (id, name, score) VALUES (1, 'Shadow', 100)");

        // SELECT via stmt_init() reads physical (empty)
        $stmt = $this->mysqli->stmt_init();
        $stmt->prepare('SELECT COUNT(*) AS cnt FROM mi_si_test');
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame(0, (int) $row['cnt']);
        $stmt->close();
    }

    /**
     * stmt_init() with bind_param also bypasses ZTD.
     */
    public function testStmtInitWithBindParamBypassesZtd(): void
    {
        // Insert via stmt_init with bind_param
        $stmt = $this->mysqli->stmt_init();
        $stmt->prepare('INSERT INTO mi_si_test (id, name, score) VALUES (?, ?, ?)');
        $id = 1;
        $name = 'Alice';
        $score = 90;
        $stmt->bind_param('isi', $id, $name, $score);
        $stmt->execute();
        $stmt->close();

        // Data is in physical DB
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT name FROM mi_si_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
        $this->mysqli->enableZtd();

        // Data is NOT in shadow
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_si_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_si_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
