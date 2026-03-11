<?php

declare(strict_types=1);

namespace Tests\Support;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoStatement;

abstract class AbstractMysqlPdoTestCase extends TestCase
{
    protected ZtdPdo $pdo;

    /**
     * Return one or more CREATE TABLE statements.
     *
     * @return string|string[]
     */
    abstract protected function getTableDDL(): string|array;

    /**
     * Return the table names used by this test (for cleanup).
     *
     * @return string[]
     */
    abstract protected function getTableNames(): array;

    private static function isDockerMode(): bool
    {
        return getenv('MYSQL_HOST') !== false;
    }

    private static function getMysqlDsn(): string
    {
        if (self::isDockerMode()) {
            $host = getenv('MYSQL_HOST');
            $port = getenv('MYSQL_PORT') ?: '3306';
            $db = getenv('MYSQL_DATABASE') ?: 'ztd_test';
            return "mysql:host={$host};port={$port};dbname={$db}";
        }
        return MySQLContainer::getDsn();
    }

    private static function getMysqlUser(): string
    {
        if (self::isDockerMode()) {
            return getenv('MYSQL_USER') ?: 'root';
        }
        return 'root';
    }

    private static function getMysqlPassword(): string
    {
        if (self::isDockerMode()) {
            return getenv('MYSQL_PASSWORD') ?: 'root';
        }
        return 'root';
    }

    public static function setUpBeforeClass(): void
    {
        if (self::isDockerMode()) {
            return;
        }
        MySQLContainer::resolveImage();
        $container = (new MySQLContainer())->withReuseMode(\Testcontainers\Containers\ReuseMode::REUSE());
        \Testcontainers\Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            self::getMysqlDsn(),
            self::getMysqlUser(),
            self::getMysqlPassword(),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        foreach ($this->getTableNames() as $table) {
            $raw->exec("DROP TABLE IF EXISTS {$table}");
        }
        foreach ((array) $this->getTableDDL() as $ddl) {
            $raw->exec($ddl);
        }

        $this->pdo = $this->createZtdConnection();

        VersionRecorder::setVersionInfo(static::class, $this->getDbVersion(), $this->getZtdVersion());
    }

    protected function createZtdConnection(): ZtdPdo
    {
        return new ZtdPdo(
            self::getMysqlDsn(),
            self::getMysqlUser(),
            self::getMysqlPassword(),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    protected function ztdExec(string $sql): int|false
    {
        return $this->pdo->exec($sql);
    }

    protected function ztdQuery(string $sql): array
    {
        return $this->pdo->query($sql)->fetchAll(PDO::FETCH_ASSOC);
    }

    protected function ztdPrepare(string $sql): ZtdPdoStatement
    {
        return $this->pdo->prepare($sql);
    }

    /**
     * Execute a prepared statement and return all rows as associative arrays.
     *
     * @param string $sql   SQL with ? placeholders
     * @param array  $params Positional parameter values
     * @return array
     */
    protected function ztdPrepareAndExecute(string $sql, array $params): array
    {
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    protected function createTable(string $ddl): void
    {
        $raw = new PDO(
            self::getMysqlDsn(),
            self::getMysqlUser(),
            self::getMysqlPassword(),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec($ddl);
    }

    protected function dropTable(string $name): void
    {
        $raw = new PDO(
            self::getMysqlDsn(),
            self::getMysqlUser(),
            self::getMysqlPassword(),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec("DROP TABLE IF EXISTS {$name}");
    }

    protected function disableZtd(): void
    {
        $this->pdo->disableZtd();
    }

    protected function enableZtd(): void
    {
        $this->pdo->enableZtd();
    }

    protected function isZtdEnabled(): bool
    {
        return $this->pdo->isZtdEnabled();
    }

    protected function ztdBeginTransaction(): bool
    {
        return $this->pdo->beginTransaction();
    }

    protected function ztdCommit(): bool
    {
        return $this->pdo->commit();
    }

    protected function ztdRollBack(): bool
    {
        return $this->pdo->rollBack();
    }

    protected function ztdInTransaction(): bool
    {
        return $this->pdo->inTransaction();
    }

    protected function getDbVersion(): string
    {
        $raw = new PDO(
            self::getMysqlDsn(),
            self::getMysqlUser(),
            self::getMysqlPassword(),
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        return $raw->query('SELECT VERSION()')->fetchColumn();
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
            if (($pkg['name'] ?? '') === 'k-kinzal/ztd-query-pdo-adapter') {
                return $pkg['version'] ?? 'unknown';
            }
        }
        return 'unknown';
    }

    public static function tearDownAfterClass(): void
    {
        // Subclasses may override for cleanup
    }
}
