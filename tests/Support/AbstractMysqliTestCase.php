<?php

declare(strict_types=1);

namespace Tests\Support;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

abstract class AbstractMysqliTestCase extends TestCase
{
    protected ZtdMysqli $mysqli;

    /**
     * @return string|string[]
     */
    abstract protected function getTableDDL(): string|array;

    /**
     * @return string[]
     */
    abstract protected function getTableNames(): array;

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
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        foreach ($this->getTableNames() as $table) {
            $raw->query("DROP TABLE IF EXISTS {$table}");
        }
        foreach ((array) $this->getTableDDL() as $ddl) {
            $raw->query($ddl);
        }
        $raw->close();

        $this->mysqli = $this->createZtdConnection();

        VersionRecorder::setVersionInfo(static::class, $this->getDbVersion(), $this->getZtdVersion());
    }

    protected function createZtdConnection(): ZtdMysqli
    {
        return new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    protected function ztdExec(string $sql): int|false
    {
        $result = $this->mysqli->query($sql);
        if ($result === false) {
            return false;
        }
        return $this->mysqli->lastAffectedRows();
    }

    protected function ztdQuery(string $sql): array
    {
        $result = $this->mysqli->query($sql);
        return $result->fetch_all(MYSQLI_ASSOC);
    }

    /**
     * Execute a prepared statement and return all rows as associative arrays.
     * Uses ? placeholders compatible with MySQLi.
     */
    protected function ztdPrepareAndExecute(string $sql, array $params): array
    {
        $stmt = $this->mysqli->prepare($sql);
        if (!empty($params)) {
            $types = '';
            foreach ($params as $p) {
                if (is_int($p)) {
                    $types .= 'i';
                } elseif (is_float($p)) {
                    $types .= 'd';
                } else {
                    $types .= 's';
                }
            }
            $stmt->bind_param($types, ...$params);
        }
        $stmt->execute();
        $result = $stmt->get_result();
        return $result->fetch_all(MYSQLI_ASSOC);
    }

    protected function createTable(string $ddl): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query($ddl);
        $raw->close();
    }

    protected function dropTable(string $name): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query("DROP TABLE IF EXISTS {$name}");
        $raw->close();
    }

    protected function disableZtd(): void
    {
        $this->mysqli->disableZtd();
    }

    protected function enableZtd(): void
    {
        $this->mysqli->enableZtd();
    }

    protected function isZtdEnabled(): bool
    {
        return $this->mysqli->isZtdEnabled();
    }

    protected function ztdBeginTransaction(): bool
    {
        return $this->mysqli->begin_transaction();
    }

    protected function ztdCommit(): bool
    {
        return $this->mysqli->commit();
    }

    protected function ztdRollBack(): bool
    {
        return $this->mysqli->rollback();
    }

    protected function getDbVersion(): string
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $result = $raw->query('SELECT VERSION()');
        $version = $result->fetch_row()[0];
        $raw->close();
        return $version;
    }

    protected function getPhpVersion(): string
    {
        return PHP_VERSION;
    }

    protected function getZtdVersion(): string
    {
        $installedFile = __DIR__ . '/../../vendor/composer/installed.json';
        if (!file_exists($installedFile)) {
            return 'unknown';
        }
        $installed = json_decode(file_get_contents($installedFile), true);
        foreach ($installed['packages'] ?? $installed as $pkg) {
            if (($pkg['name'] ?? '') === 'k-kinzal/ztd-query-mysqli-adapter') {
                return $pkg['version'] ?? 'unknown';
            }
        }
        return 'unknown';
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        // Subclasses may override for cleanup
    }
}
