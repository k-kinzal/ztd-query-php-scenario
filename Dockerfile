ARG PHP_VERSION=8.3
ARG PHP_IMAGE=php:${PHP_VERSION}-cli-bookworm
FROM ${PHP_IMAGE}

ARG SQLITE_VERSION=system
ARG SQLITE_URL
ARG SQLITE_SHA256

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    unzip \
    libpq-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# The official image's SQLite extensions dynamically link libsqlite3.so.0.
# Install the selected library and verify what PHP actually loads.
COPY docker/install-sqlite.sh /tmp/install-sqlite.sh
RUN sh /tmp/install-sqlite.sh \
    && docker-php-ext-install -j2 \
        pdo_mysql \
        pdo_pgsql \
        mysqli \
    && php -r '$v = (new PDO("sqlite::memory:"))->query("SELECT sqlite_version()")->fetchColumn(); if (getenv("SQLITE_VERSION") !== "system" && $v !== getenv("SQLITE_VERSION")) { fwrite(STDERR, "Wrong PDO SQLite: $v\n"); exit(1); } if (SQLite3::version()["versionString"] !== $v) { exit(1); }' \
    && rm /tmp/install-sqlite.sh

# Install Composer
COPY --from=composer:2 /usr/bin/composer /usr/local/bin/composer

WORKDIR /app

COPY composer.json composer.lock ./

# The lock file is resolved for PHP 8.1, the oldest supported runtime.
# Every matrix image installs the same ZTD commits and test dependencies.
RUN composer install --no-interaction --no-progress --prefer-dist \
    && composer check-platform-reqs

COPY . .

# Default: run all tests
ENTRYPOINT ["php", "vendor/bin/phpunit"]
CMD ["--testsuite", "Scenario"]
