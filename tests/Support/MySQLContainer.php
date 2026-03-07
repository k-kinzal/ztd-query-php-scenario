<?php

declare(strict_types=1);

namespace Tests\Support;

use Testcontainers\Containers\ContainerInstance;
use Testcontainers\Containers\GenericContainer\GenericContainer;
use Testcontainers\Containers\WaitStrategy\PDO\MySQLDSN;
use Testcontainers\Containers\WaitStrategy\PDO\PDOConnectWaitStrategy;
use Testcontainers\Hook\AfterStartHook;

class MySQLContainer extends GenericContainer
{
    use AfterStartHook;
    protected static $IMAGE = 'mysql:8.0';
    protected static $AUTO_REMOVE_ON_EXIT = true;

    protected static $EXPOSED_PORTS = [3306];

    protected static $ENVIRONMENTS = [
        'MYSQL_ROOT_PASSWORD' => 'root',
        'MYSQL_DATABASE' => 'test',
    ];

    private static ?ContainerInstance $lastInstance = null;

    public function afterStart($instance): void
    {
        self::$lastInstance = $instance;
    }

    public static function getLastInstance(): ContainerInstance
    {
        if (self::$lastInstance === null) {
            throw new \RuntimeException('No MySQL container has been started');
        }
        return self::$lastInstance;
    }

    public static function getDsn(): string
    {
        $port = self::getLastInstance()->getMappedPort(3306);
        return sprintf('mysql:host=127.0.0.1;port=%d;dbname=test', $port);
    }

    public static function getHost(): string
    {
        return '127.0.0.1';
    }

    public static function getPort(): int
    {
        return self::getLastInstance()->getMappedPort(3306);
    }

    protected function registerWaitStrategy($provider): void
    {
        parent::registerWaitStrategy($provider);
        $strategy = (new PDOConnectWaitStrategy())
            ->withDsn((new MySQLDSN())->withDbname('test'))
            ->withUsername('root')
            ->withPassword('root')
            ->withTimeoutSeconds(60);
        $provider->register('pdo_connect', $strategy);
    }

    protected static $WAIT_STRATEGY = 'pdo_connect';
}
