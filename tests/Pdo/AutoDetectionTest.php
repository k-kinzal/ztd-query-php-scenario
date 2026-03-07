<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class AutoDetectionTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    public function testAutoDetectsMysqlDriver(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->assertTrue($pdo->isZtdEnabled());
    }

    public function testAutoDetectsSqliteDriver(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $pdo = ZtdPdo::fromPdo($raw);

        $this->assertTrue($pdo->isZtdEnabled());
    }

    public function testFromPdoWithMysqlDriver(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $pdo = ZtdPdo::fromPdo($raw);

        $this->assertTrue($pdo->isZtdEnabled());
    }

    public function testUnsupportedDriverThrowsException(): void
    {
        // ODBC driver (if available) or simulated unsupported driver
        // Since we can't easily create an unsupported PDO driver instance,
        // we verify the auto-detection works for known drivers and document
        // that unsupported drivers throw RuntimeException
        $this->expectException(\RuntimeException::class);

        // Use reflection to test the detectFactory path with an unsupported driver
        $raw = new PDO('sqlite::memory:');
        $ref = new \ReflectionClass(ZtdPdo::class);
        $method = $ref->getMethod('detectFactory');

        // Mock a PDO with an unsupported driver name by using a subclass
        $mockPdo = new class('sqlite::memory:') extends PDO {
            public function getAttribute(int $attribute): mixed
            {
                if ($attribute === PDO::ATTR_DRIVER_NAME) {
                    return 'unsupported_driver';
                }
                return parent::getAttribute($attribute);
            }
        };

        $method->invoke(null, $mockPdo);
    }
}
