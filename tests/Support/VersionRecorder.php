<?php

declare(strict_types=1);

namespace Tests\Support;

use PHPUnit\Event\Test\Finished as TestFinished;
use PHPUnit\Event\Test\FinishedSubscriber as TestFinishedSubscriber;
use PHPUnit\Event\TestRunner\Finished as RunnerFinished;
use PHPUnit\Event\TestRunner\FinishedSubscriber as RunnerFinishedSubscriber;
use PHPUnit\Runner\Extension\Extension;
use PHPUnit\Runner\Extension\Facade;
use PHPUnit\Runner\Extension\ParameterCollection;
use PHPUnit\TextUI\Configuration\Configuration;

/**
 * PHPUnit extension that records {testClass, phpVersion, dbVersion, ztdVersion}
 * per test run into spec/verification-log.json.
 *
 * Register in phpunit.xml:
 *   <extensions>
 *       <bootstrap class="Tests\Support\VersionRecorder"/>
 *   </extensions>
 */
final class VersionRecorder implements Extension
{
    /** @var array<string, array{phpVersion: string, dbVersion: string, ztdVersion: string, adapter: string}> */
    private static array $entries = [];

    /** @var array<string, array{dbVersion: string, ztdVersion: string}> Version info set by base class setUp(), before TestFinished fires */
    private static array $versionInfo = [];

    /** @var string|null */
    private static ?string $outputPath = null;

    public function bootstrap(Configuration $configuration, Facade $facade, ParameterCollection $parameters): void
    {
        self::$outputPath = dirname(__DIR__, 2) . '/spec/verification-log.json';
        self::$entries = [];
        self::$versionInfo = [];

        $facade->registerSubscriber(new class implements TestFinishedSubscriber {
            public function notify(TestFinished $event): void
            {
                VersionRecorder::recordTest($event);
            }
        });

        $facade->registerSubscriber(new class implements RunnerFinishedSubscriber {
            public function notify(RunnerFinished $event): void
            {
                VersionRecorder::flush();
            }
        });
    }

    public static function recordTest(TestFinished $event): void
    {
        $testClassName = $event->test()->className();

        // Only record once per test class
        if (isset(self::$entries[$testClassName])) {
            return;
        }

        $adapter = self::detectAdapter($testClassName);

        // Use version info set by base class setUp() if available,
        // otherwise auto-detect from running containers
        $dbVersion = self::$versionInfo[$testClassName]['dbVersion']
            ?? self::autoDetectDbVersion($adapter);
        $ztdVersion = self::$versionInfo[$testClassName]['ztdVersion'] ?? self::detectZtdVersion($adapter);

        self::$entries[$testClassName] = [
            'phpVersion' => PHP_VERSION,
            'dbVersion' => $dbVersion,
            'ztdVersion' => $ztdVersion,
            'adapter' => $adapter,
            'timestamp' => date('c'),
        ];
    }

    public static function flush(): void
    {
        if (self::$outputPath === null || empty(self::$entries)) {
            return;
        }

        // Merge with existing log if present
        $existing = [];
        if (file_exists(self::$outputPath)) {
            $content = file_get_contents(self::$outputPath);
            if ($content !== false) {
                $existing = json_decode($content, true) ?? [];
            }
        }

        $merged = array_merge($existing, self::$entries);

        // Sort by class name for readability
        ksort($merged);

        file_put_contents(
            self::$outputPath,
            json_encode($merged, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n"
        );
    }

    /**
     * Set version info from a test base class setUp method.
     *
     * Called during setUp() (before TestFinished fires), so the info is stored
     * separately and merged when recordTest() creates the entry.
     */
    public static function setVersionInfo(string $testClassName, string $dbVersion, string $ztdVersion): void
    {
        self::$versionInfo[$testClassName] = [
            'dbVersion' => $dbVersion,
            'ztdVersion' => $ztdVersion,
        ];

        // Also update existing entry if already recorded
        if (isset(self::$entries[$testClassName])) {
            self::$entries[$testClassName]['dbVersion'] = $dbVersion;
            self::$entries[$testClassName]['ztdVersion'] = $ztdVersion;
        }
    }

    private static function autoDetectDbVersion(string $adapter): string
    {
        try {
            return match ($adapter) {
                'mysqli' => self::detectMysqlVersionViaMysqli(),
                'mysql-pdo' => self::detectMysqlVersionViaPdo(),
                'postgres-pdo' => self::detectPostgresVersionViaPdo(),
                'sqlite-pdo' => self::detectSqliteVersion(),
                default => 'unknown',
            };
        } catch (\Throwable) {
            return 'unknown';
        }
    }

    private static function detectMysqlVersionViaMysqli(): string
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

    private static function detectMysqlVersionViaPdo(): string
    {
        $raw = new \PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION],
        );
        return $raw->query('SELECT VERSION()')->fetchColumn();
    }

    private static function detectPostgresVersionViaPdo(): string
    {
        $raw = new \PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION],
        );
        return $raw->query('SHOW server_version')->fetchColumn();
    }

    private static function detectSqliteVersion(): string
    {
        $raw = new \PDO('sqlite::memory:');
        return $raw->query('SELECT sqlite_version()')->fetchColumn();
    }

    private static function detectAdapter(string $className): string
    {
        if (str_contains($className, '\\Mysqli\\')) {
            return 'mysqli';
        }
        if (str_contains($className, 'Mysql') || str_contains($className, 'mysql')) {
            return 'mysql-pdo';
        }
        if (str_contains($className, 'Postgres') || str_contains($className, 'postgres')) {
            return 'postgres-pdo';
        }
        if (str_contains($className, 'Sqlite') || str_contains($className, 'sqlite')) {
            return 'sqlite-pdo';
        }
        return 'unknown';
    }

    private static function detectZtdVersion(string $adapter): string
    {
        $installedFile = dirname(__DIR__, 2) . '/vendor/composer/installed.json';
        if (!file_exists($installedFile)) {
            return 'unknown';
        }

        $installed = json_decode(file_get_contents($installedFile), true);
        $packageName = match ($adapter) {
            'mysqli' => 'k-kinzal/ztd-query-mysqli-adapter',
            default => 'k-kinzal/ztd-query-pdo-adapter',
        };

        foreach ($installed['packages'] ?? $installed as $pkg) {
            if (($pkg['name'] ?? '') === $packageName) {
                return $pkg['version'] ?? 'unknown';
            }
        }
        return 'unknown';
    }
}
