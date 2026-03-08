<?php

declare(strict_types=1);

namespace Tests\Support;

use Testcontainers\Containers\ContainerInstance;
use Testcontainers\Containers\GenericContainer\GenericContainer;
use Testcontainers\Containers\WaitStrategy\PDO\PDOConnectWaitStrategy;
use Testcontainers\Hook\AfterStartHook;

class PostgreSQLContainer extends GenericContainer
{
    use AfterStartHook;

    protected static $IMAGE = 'postgres:16';
    protected static $AUTO_REMOVE_ON_EXIT = true;

    /**
     * Resolve the container image from POSTGRES_IMAGE env var.
     * Call before creating the container instance.
     */
    public static function resolveImage(): void
    {
        $image = getenv('POSTGRES_IMAGE');
        if ($image !== false && $image !== '') {
            static::$IMAGE = $image;
        }
    }
    protected static $EXPOSED_PORTS = [5432];
    protected static $ENVIRONMENTS = [
        'POSTGRES_USER' => 'test',
        'POSTGRES_PASSWORD' => 'test',
        'POSTGRES_DB' => 'test',
    ];

    private static ?ContainerInstance $lastInstance = null;

    public function afterStart($instance): void
    {
        self::$lastInstance = $instance;
    }

    public static function getLastInstance(): ContainerInstance
    {
        if (self::$lastInstance === null) {
            throw new \RuntimeException('No PostgreSQL container has been started');
        }
        return self::$lastInstance;
    }

    public static function getDsn(): string
    {
        $port = self::getLastInstance()->getMappedPort(5432);
        return sprintf('pgsql:host=127.0.0.1;port=%d;dbname=test', $port);
    }

    public static function getHost(): string
    {
        return '127.0.0.1';
    }

    public static function getPort(): int
    {
        return self::getLastInstance()->getMappedPort(5432);
    }

    protected function registerWaitStrategy($provider): void
    {
        parent::registerWaitStrategy($provider);
        $strategy = (new PDOConnectWaitStrategy())
            ->withDsn((new PostgreSQLDSN())->withDbname('test'))
            ->withUsername('test')
            ->withPassword('test')
            ->withTimeoutSeconds(60);
        $provider->register('pdo_connect', $strategy);
    }

    protected static $WAIT_STRATEGY = 'pdo_connect';
}
