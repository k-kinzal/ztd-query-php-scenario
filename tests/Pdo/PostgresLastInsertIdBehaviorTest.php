<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PDOException;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests lastInsertId() behavior with ZTD on PostgreSQL.
 *
 * PostgreSQL uses SERIAL columns backed by sequences.
 * ZtdPdo::lastInsertId() delegates to the inner PDO connection.
 * Since ZTD rewrites INSERT to CTE-based SELECT, the physical sequence
 * is NOT advanced by shadow INSERTs.
 *
 * On PostgreSQL, calling lastInsertId() when the sequence hasn't been
 * advanced in the current session throws a PDOException — unlike MySQL
 * which returns "0".
 */
class PostgresLastInsertIdBehaviorTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
        $raw->exec('DROP TABLE IF EXISTS pg_lid_test');
        $raw->exec('CREATE TABLE pg_lid_test (id SERIAL PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(PostgreSQLContainer::getDsn(), 'test', 'test');
    }

    /**
     * lastInsertId() with sequence name throws PDOException after shadow INSERT.
     *
     * Since ZTD didn't advance the sequence (no physical INSERT), PostgreSQL
     * throws "currval of sequence is not yet defined in this session".
     */
    public function testLastInsertIdThrowsAfterShadowInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_lid_test (name, score) VALUES ('Alice', 90)");

        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('not yet defined in this session');
        $this->pdo->lastInsertId('pg_lid_test_id_seq');
    }

    /**
     * lastInsertId() without sequence name throws on PostgreSQL.
     *
     * PostgreSQL's lastval() also hasn't been set since no physical
     * INSERT occurred.
     */
    public function testLastInsertIdWithoutSequenceThrows(): void
    {
        $this->pdo->exec("INSERT INTO pg_lid_test (name, score) VALUES ('Alice', 90)");

        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('not yet defined in this session');
        $this->pdo->lastInsertId();
    }

    /**
     * Shadow data IS accessible despite lastInsertId throwing.
     */
    public function testShadowDataAccessibleDespiteException(): void
    {
        $this->pdo->exec("INSERT INTO pg_lid_test (name, score) VALUES ('Alice', 90)");

        // lastInsertId throws
        try {
            $this->pdo->lastInsertId('pg_lid_test_id_seq');
            $this->fail('Expected PDOException');
        } catch (PDOException $e) {
            $this->assertStringContainsString('not yet defined', $e->getMessage());
        }

        // But shadow data IS queryable
        $stmt = $this->pdo->query("SELECT name FROM pg_lid_test WHERE name = 'Alice'");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * lastInsertId works correctly when ZTD is disabled.
     */
    public function testLastInsertIdWorksWithZtdDisabled(): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec("INSERT INTO pg_lid_test (name, score) VALUES ('Physical', 100)");

        $id = $this->pdo->lastInsertId('pg_lid_test_id_seq');
        $this->assertGreaterThan(0, (int) $id);
    }

    /**
     * Explicit ID in shadow INSERT — sequence still not advanced.
     */
    public function testExplicitIdDoesNotAdvanceSequence(): void
    {
        $this->pdo->exec("INSERT INTO pg_lid_test (id, name, score) VALUES (999, 'Alice', 90)");

        $this->expectException(PDOException::class);
        $this->expectExceptionMessage('not yet defined in this session');
        $this->pdo->lastInsertId('pg_lid_test_id_seq');
    }

    /**
     * Physical isolation verification.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_lid_test (id, name, score) VALUES (1, 'Alice', 90)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_lid_test WHERE id = 1 AND name = \'Alice\'');
        // The physical row from testLastInsertIdWorksWithZtdDisabled may exist,
        // but this specific row (id=1, name='Alice') should not be in the physical table
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(PostgreSQLContainer::getDsn(), 'test', 'test');
            $raw->exec('DROP TABLE IF EXISTS pg_lid_test');
        } catch (\Exception $e) {
        }
    }
}
