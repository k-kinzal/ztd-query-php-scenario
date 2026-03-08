<?php

declare(strict_types=1);

namespace Tests\Support;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoStatement;

abstract class AbstractPostgresPdoTestCase extends TestCase
{
    protected ZtdPdo $pdo;

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
        PostgreSQLContainer::resolveImage();
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        foreach ($this->getTableNames() as $table) {
            $raw->exec("DROP TABLE IF EXISTS {$table} CASCADE");
        }
        foreach ((array) $this->getTableDDL() as $ddl) {
            $raw->exec($ddl);
        }

        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
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

    protected function ztdPrepareAndExecute(string $sql, array $params): array
    {
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute($params);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    protected function createTable(string $ddl): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec($ddl);
    }

    protected function dropTable(string $name): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec("DROP TABLE IF EXISTS {$name} CASCADE");
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
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        return $raw->query('SHOW server_version')->fetchColumn();
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
