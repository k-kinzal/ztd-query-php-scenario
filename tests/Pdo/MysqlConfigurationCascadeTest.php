<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a hierarchical key-value configuration system with defaults,
 * overrides, and environment-specific settings through ZTD shadow store (MySQL PDO).
 * Covers cascade resolution, priority-based overrides, and effective config reports.
 * @spec SPEC-10.2.66
 */
class MysqlConfigurationCascadeTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_cc_defaults (
                id INT PRIMARY KEY,
                config_key VARCHAR(255),
                config_value VARCHAR(255),
                description VARCHAR(255)
            )',
            'CREATE TABLE mp_cc_overrides (
                id INT PRIMARY KEY,
                config_key VARCHAR(255),
                environment VARCHAR(255),
                config_value VARCHAR(255),
                priority INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_cc_overrides', 'mp_cc_defaults'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 5 default configs
        $this->pdo->exec("INSERT INTO mp_cc_defaults VALUES (1, 'app.name', 'MyApp', 'Application name')");
        $this->pdo->exec("INSERT INTO mp_cc_defaults VALUES (2, 'app.debug', 'false', 'Debug mode flag')");
        $this->pdo->exec("INSERT INTO mp_cc_defaults VALUES (3, 'db.host', 'localhost', 'Database host')");
        $this->pdo->exec("INSERT INTO mp_cc_defaults VALUES (4, 'db.port', '3306', 'Database port')");
        $this->pdo->exec("INSERT INTO mp_cc_defaults VALUES (5, 'cache.ttl', '3600', 'Cache time-to-live in seconds')");

        // 4 overrides
        $this->pdo->exec("INSERT INTO mp_cc_overrides VALUES (1, 'app.debug', 'staging', 'true', 10)");
        $this->pdo->exec("INSERT INTO mp_cc_overrides VALUES (2, 'app.debug', 'production', 'false', 10)");
        $this->pdo->exec("INSERT INTO mp_cc_overrides VALUES (3, 'db.host', 'production', 'db.prod.internal', 10)");
        $this->pdo->exec("INSERT INTO mp_cc_overrides VALUES (4, 'cache.ttl', 'staging', '60', 5)");
    }

    /**
     * Basic key-value retrieval from defaults table.
     */
    public function testDefaultConfigLookup(): void
    {
        $rows = $this->ztdQuery(
            "SELECT config_key, config_value
             FROM mp_cc_defaults
             ORDER BY config_key"
        );

        $this->assertCount(5, $rows);
        $this->assertSame('app.debug', $rows[0]['config_key']);
        $this->assertSame('false', $rows[0]['config_value']);
        $this->assertSame('app.name', $rows[1]['config_key']);
        $this->assertSame('MyApp', $rows[1]['config_value']);
        $this->assertSame('cache.ttl', $rows[2]['config_key']);
        $this->assertSame('3600', $rows[2]['config_value']);
        $this->assertSame('db.host', $rows[3]['config_key']);
        $this->assertSame('localhost', $rows[3]['config_value']);
        $this->assertSame('db.port', $rows[4]['config_key']);
        $this->assertSame('3306', $rows[4]['config_value']);
    }

    /**
     * LEFT JOIN defaults with overrides; COALESCE picks override when present.
     */
    public function testOverrideResolution(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.config_key,
                    COALESCE(o.config_value, d.config_value) AS effective_value
             FROM mp_cc_defaults d
             LEFT JOIN mp_cc_overrides o ON o.config_key = d.config_key AND o.environment = ?
             ORDER BY d.config_key",
            ['staging']
        );

        $this->assertCount(5, $rows);
        // app.debug overridden to 'true' in staging
        $this->assertSame('app.debug', $rows[0]['config_key']);
        $this->assertSame('true', $rows[0]['effective_value']);
        // app.name has no staging override, falls back to default
        $this->assertSame('app.name', $rows[1]['config_key']);
        $this->assertSame('MyApp', $rows[1]['effective_value']);
        // cache.ttl overridden to '60' in staging
        $this->assertSame('cache.ttl', $rows[2]['config_key']);
        $this->assertSame('60', $rows[2]['effective_value']);
        // db.host has no staging override
        $this->assertSame('db.host', $rows[3]['config_key']);
        $this->assertSame('localhost', $rows[3]['effective_value']);
    }

    /**
     * Verify different values per environment for the same key.
     */
    public function testEnvironmentSpecificConfig(): void
    {
        $staging = $this->ztdPrepareAndExecute(
            "SELECT config_value FROM mp_cc_overrides
             WHERE config_key = ? AND environment = ?",
            ['app.debug', 'staging']
        );
        $this->assertCount(1, $staging);
        $this->assertSame('true', $staging[0]['config_value']);

        $production = $this->ztdPrepareAndExecute(
            "SELECT config_value FROM mp_cc_overrides
             WHERE config_key = ? AND environment = ?",
            ['app.debug', 'production']
        );
        $this->assertCount(1, $production);
        $this->assertSame('false', $production[0]['config_value']);
    }

    /**
     * When multiple overrides exist for a key in an environment,
     * the highest priority wins.
     */
    public function testPriorityBasedOverride(): void
    {
        // Add a second staging override for cache.ttl with higher priority
        $this->pdo->exec("INSERT INTO mp_cc_overrides VALUES (5, 'cache.ttl', 'staging', '30', 20)");

        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.config_key,
                    COALESCE(
                        (SELECT o.config_value FROM mp_cc_overrides o
                         WHERE o.config_key = d.config_key AND o.environment = ?
                         ORDER BY o.priority DESC LIMIT 1),
                        d.config_value
                    ) AS effective_value
             FROM mp_cc_defaults d
             WHERE d.config_key = ?",
            ['staging', 'cache.ttl']
        );

        $this->assertCount(1, $rows);
        // priority 20 override ('30') wins over priority 5 override ('60')
        $this->assertSame('30', $rows[0]['effective_value']);
    }

    /**
     * INSERT a new override and verify it takes precedence over default.
     */
    public function testAddOverride(): void
    {
        // Verify db.port uses default for production
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.config_key,
                    COALESCE(o.config_value, d.config_value) AS effective_value
             FROM mp_cc_defaults d
             LEFT JOIN mp_cc_overrides o ON o.config_key = d.config_key AND o.environment = ?
             WHERE d.config_key = ?",
            ['production', 'db.port']
        );
        $this->assertSame('3306', $rows[0]['effective_value']);

        // Add a production override for db.port
        $this->pdo->exec("INSERT INTO mp_cc_overrides VALUES (5, 'db.port', 'production', '5432', 10)");

        // Verify override now takes precedence
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.config_key,
                    COALESCE(o.config_value, d.config_value) AS effective_value
             FROM mp_cc_defaults d
             LEFT JOIN mp_cc_overrides o ON o.config_key = d.config_key AND o.environment = ?
             WHERE d.config_key = ?",
            ['production', 'db.port']
        );
        $this->assertSame('5432', $rows[0]['effective_value']);
    }

    /**
     * DELETE an override and verify the default is returned.
     */
    public function testRemoveOverrideRestoresDefault(): void
    {
        // Verify staging override exists for cache.ttl
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.config_key,
                    COALESCE(o.config_value, d.config_value) AS effective_value
             FROM mp_cc_defaults d
             LEFT JOIN mp_cc_overrides o ON o.config_key = d.config_key AND o.environment = ?
             WHERE d.config_key = ?",
            ['staging', 'cache.ttl']
        );
        $this->assertSame('60', $rows[0]['effective_value']);

        // Remove the staging override for cache.ttl
        $affected = $this->pdo->exec("DELETE FROM mp_cc_overrides WHERE config_key = 'cache.ttl' AND environment = 'staging'");
        $this->assertSame(1, $affected);

        // Verify default is now returned
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.config_key,
                    COALESCE(o.config_value, d.config_value) AS effective_value
             FROM mp_cc_defaults d
             LEFT JOIN mp_cc_overrides o ON o.config_key = d.config_key AND o.environment = ?
             WHERE d.config_key = ?",
            ['staging', 'cache.ttl']
        );
        $this->assertSame('3600', $rows[0]['effective_value']);
    }

    /**
     * Full report: all keys with effective values resolved from the cascade,
     * using LEFT JOIN + COALESCE + subquery for highest priority.
     */
    public function testEffectiveConfigReport(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.config_key,
                    d.config_value AS default_value,
                    COALESCE(
                        (SELECT o.config_value FROM mp_cc_overrides o
                         WHERE o.config_key = d.config_key AND o.environment = ?
                         ORDER BY o.priority DESC LIMIT 1),
                        d.config_value
                    ) AS effective_value
             FROM mp_cc_defaults d
             ORDER BY d.config_key",
            ['production']
        );

        $this->assertCount(5, $rows);

        // app.debug: default 'false', production override 'false' (same value)
        $this->assertSame('app.debug', $rows[0]['config_key']);
        $this->assertSame('false', $rows[0]['default_value']);
        $this->assertSame('false', $rows[0]['effective_value']);

        // app.name: no production override, uses default
        $this->assertSame('app.name', $rows[1]['config_key']);
        $this->assertSame('MyApp', $rows[1]['effective_value']);

        // cache.ttl: no production override, uses default '3600'
        $this->assertSame('cache.ttl', $rows[2]['config_key']);
        $this->assertSame('3600', $rows[2]['effective_value']);

        // db.host: production override 'db.prod.internal'
        $this->assertSame('db.host', $rows[3]['config_key']);
        $this->assertSame('localhost', $rows[3]['default_value']);
        $this->assertSame('db.prod.internal', $rows[3]['effective_value']);

        // db.port: no production override, uses default '3306'
        $this->assertSame('db.port', $rows[4]['config_key']);
        $this->assertSame('3306', $rows[4]['effective_value']);
    }
}
