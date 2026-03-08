<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests unknown schema behavior workflows on MySQLi: INSERT into unreflected table,
 * then UPDATE/DELETE the inserted rows. After INSERT, the table is registered
 * in the shadow store but without PK info, so UPDATE throws RuntimeException.
 * @spec SPEC-7.1, SPEC-7.2, SPEC-7.3, SPEC-7.4
 */
class UnknownSchemaWorkflowTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        MySQLContainer::resolveImage();
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root', 'root', 'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS late_wf');
        $raw->close();
    }

    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): ZtdMysqli
    {
        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root', 'root', 'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root', 'root', 'test',
            MySQLContainer::getPort(),
        );
        $raw->query('CREATE TABLE late_wf (id INT PRIMARY KEY, val VARCHAR(255), score INT)');
        $raw->query("INSERT INTO late_wf VALUES (1, 'physical', 10)");
        $raw->close();

        return $mysqli;
    }

    /**
     * Passthrough: INSERT into shadow, then UPDATE throws RuntimeException
     * because PK info was not reflected for the unreflected table.
     */
    public function testPassthroughInsertThenUpdateThrows(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $mysqli->query("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        $result = $mysqli->query('SELECT val FROM late_wf WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame('shadow', $row['val']);

        // UPDATE throws because PK info was not reflected
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $mysqli->query("UPDATE late_wf SET val = 'updated' WHERE id = 1");
    }

    /**
     * Passthrough: INSERT registers table, so DELETE operates on shadow data.
     */
    public function testPassthroughInsertThenDeleteOperatesOnShadow(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $mysqli->query("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // After INSERT, DELETE operates on shadow data
        $mysqli->query("DELETE FROM late_wf WHERE id = 2");

        $result = $mysqli->query('SELECT val FROM late_wf WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertNull($row);

        $mysqli->close();
    }

    /**
     * Exception: INSERT then UPDATE throws RuntimeException (not ZtdMysqliException)
     * because the error comes from ShadowStore before unknown schema check.
     */
    public function testExceptionInsertThenUpdateThrows(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $mysqli->query("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // UPDATE throws RuntimeException from ShadowStore, not ZtdMysqliException
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $mysqli->query("UPDATE late_wf SET val = 'updated' WHERE id = 2");
    }

    /**
     * Exception: After INSERT, DELETE behavior depends on shadow registration.
     */
    public function testExceptionInsertThenDeleteBehavior(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $mysqli->query("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        try {
            $mysqli->query("DELETE FROM late_wf WHERE id = 2");
            // If DELETE succeeds, shadow store registered the table
            $result = $mysqli->query('SELECT val FROM late_wf WHERE id = 2');
            $row = $result->fetch_assoc();
            $this->assertNull($row, 'DELETE removed shadow row');
        } catch (\RuntimeException $e) {
            // If DELETE throws, Exception mode prevents schema registration
            $this->assertMatchesRegularExpression('/unknown table/i', $e->getMessage());
        }

        $mysqli->close();
    }

    /**
     * EmptyResult: UPDATE throws RuntimeException (PK info missing).
     */
    public function testEmptyResultInsertThenUpdateThrows(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $mysqli->query("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $mysqli->query("UPDATE late_wf SET val = 'updated' WHERE id = 1");
    }

    /**
     * EmptyResult: DELETE operates on shadow data after INSERT.
     */
    public function testEmptyResultInsertThenDelete(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $mysqli->query("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");
        $mysqli->query("DELETE FROM late_wf WHERE id = 2");

        $result = $mysqli->query('SELECT val FROM late_wf WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertNull($row);

        $mysqli->close();
    }

    /**
     * Notice: INSERT then UPDATE throws RuntimeException (PK info missing).
     */
    public function testNoticeInsertThenUpdateThrows(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $mysqli->query("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $mysqli->query("UPDATE late_wf SET val = 'updated' WHERE id = 1");
    }

    /**
     * SELECT passes through regardless of mode.
     */
    public function testSelectPassesThroughAllModes(): void
    {
        foreach (UnknownSchemaBehavior::cases() as $behavior) {
            $this->setUp();
            $mysqli = $this->createAdapterThenTable($behavior);

            $result = $mysqli->query('SELECT val FROM late_wf WHERE id = 1');
            $row = $result->fetch_assoc();
            $this->assertSame('physical', $row['val'], "SELECT should pass through in {$behavior->name} mode");

            $mysqli->close();
        }
    }

    protected function tearDown(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root', 'root', 'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS late_wf');
        $raw->close();
    }
}
