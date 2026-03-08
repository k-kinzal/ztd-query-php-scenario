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

    /** @var string|null */
    private static ?string $outputPath = null;

    public function bootstrap(Configuration $configuration, Facade $facade, ParameterCollection $parameters): void
    {
        self::$outputPath = dirname(__DIR__, 2) . '/spec/verification-log.json';
        self::$entries = [];

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

        self::$entries[$testClassName] = [
            'phpVersion' => PHP_VERSION,
            'dbVersion' => 'pending', // Populated by base class setUp if available
            'ztdVersion' => self::detectZtdVersion($adapter),
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
     */
    public static function setVersionInfo(string $testClassName, string $dbVersion, string $ztdVersion): void
    {
        if (isset(self::$entries[$testClassName])) {
            self::$entries[$testClassName]['dbVersion'] = $dbVersion;
            self::$entries[$testClassName]['ztdVersion'] = $ztdVersion;
        }
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
