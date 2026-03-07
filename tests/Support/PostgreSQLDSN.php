<?php

declare(strict_types=1);

namespace Tests\Support;

use Testcontainers\Containers\WaitStrategy\PDO\DSN;
use Testcontainers\Utility\Stringable;

class PostgreSQLDSN implements DSN, Stringable
{
    private ?string $host = null;
    private ?int $port = null;
    private ?string $dbname = null;

    public function __toString()
    {
        return $this->toString();
    }

    public function withHost($host)
    {
        $this->host = $host;
        return $this;
    }

    public function getHost()
    {
        return $this->host;
    }

    public function withPort($port)
    {
        $this->port = $port;
        return $this;
    }

    public function getPort()
    {
        return $this->port;
    }

    public function withDbname(string $dbname): self
    {
        $this->dbname = $dbname;
        return $this;
    }

    public function toString()
    {
        if ($this->host === null) {
            throw new \LogicException('Host is required');
        }
        $dsn = sprintf('pgsql:host=%s;', $this->host);
        if ($this->port !== null) {
            $dsn .= 'port=' . $this->port . ';';
        }
        if ($this->dbname !== null) {
            $dsn .= 'dbname=' . $this->dbname . ';';
        }
        return $dsn;
    }

    public function requiresHostPort()
    {
        return true;
    }
}
