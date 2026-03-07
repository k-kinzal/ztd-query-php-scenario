<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests type handling in the shadow store on PostgreSQL PDO.
 */
class PostgresTypeHandlingTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS type_test');
        $raw->exec('CREATE TABLE type_test (id INT PRIMARY KEY, float_val DOUBLE PRECISION, bool_val BOOLEAN, date_val DATE, long_text TEXT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testFloatPrecision(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, float_val) VALUES (1, 3.14159265358979)");

        $stmt = $this->pdo->query('SELECT float_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(3.14159265358979, (float) $row['float_val'], 0.0001);
    }

    public function testBooleanTrueWorks(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (1, TRUE)");

        $stmt = $this->pdo->query('SELECT bool_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // Shadow store stores TRUE correctly
        $this->assertTrue(in_array($row['bool_val'], [true, 't', '1', 1], true));
    }

    /**
     * Bug: PostgreSQL BOOLEAN FALSE is stored as empty string in the shadow store.
     * The CTE rewriter then attempts CAST('' AS BOOLEAN) which PostgreSQL rejects.
     */
    public function testBooleanFalseThrowsOnPostgres(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (1, TRUE)");

        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('invalid input syntax for type boolean');
        // Inserting FALSE causes the CTE rebuild to fail on the next query
        $this->pdo->exec("INSERT INTO type_test (id, bool_val) VALUES (2, FALSE)");
        $this->pdo->query('SELECT * FROM type_test');
    }

    public function testDateStringStorage(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, date_val) VALUES (1, '2026-03-07')");

        $stmt = $this->pdo->query('SELECT date_val FROM type_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('2026-03-07', $row['date_val']);
    }

    public function testLongTextStorage(): void
    {
        $longText = str_repeat('abcdefghij', 500);
        $stmt = $this->pdo->prepare('INSERT INTO type_test (id, long_text) VALUES (?, ?)');
        $stmt->execute([1, $longText]);

        $select = $this->pdo->query('SELECT long_text FROM type_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame($longText, $row['long_text']);
    }

    public function testUnicodeStrings(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO type_test (id, long_text) VALUES (?, ?)');
        $stmt->execute([1, '日本語テスト 🎉 émojis café']);

        $select = $this->pdo->query('SELECT long_text FROM type_test WHERE id = 1');
        $row = $select->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('日本語テスト 🎉 émojis café', $row['long_text']);
    }

    public function testMultiRowInsert(): void
    {
        $this->pdo->exec("INSERT INTO type_test (id, float_val) VALUES (1, 1.1), (2, 2.2), (3, 3.3)");

        $stmt = $this->pdo->query('SELECT COUNT(*) AS cnt FROM type_test');
        $this->assertSame(3, (int) $stmt->fetch(PDO::FETCH_ASSOC)['cnt']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS type_test');
    }
}
