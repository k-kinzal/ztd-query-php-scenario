<?php

declare(strict_types=1);

namespace Tests\Support;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoStatement;

abstract class AbstractSqlitePdoTestCase extends TestCase
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

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        foreach ((array) $this->getTableDDL() as $ddl) {
            $raw->exec($ddl);
        }

        $this->pdo = $this->createZtdConnection($raw);

        VersionRecorder::setVersionInfo(static::class, $this->getDbVersion(), $this->getZtdVersion());
    }

    protected function createZtdConnection(\PDO $rawPdo): ZtdPdo
    {
        return ZtdPdo::fromPdo($rawPdo);
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
        $this->pdo->disableZtd();
        $this->pdo->exec($ddl);
        $this->pdo->enableZtd();
    }

    protected function dropTable(string $name): void
    {
        $this->pdo->disableZtd();
        $this->pdo->exec("DROP TABLE IF EXISTS {$name}");
        $this->pdo->enableZtd();
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
        $raw = new PDO('sqlite::memory:');
        return $raw->query('SELECT sqlite_version()')->fetchColumn();
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
}
