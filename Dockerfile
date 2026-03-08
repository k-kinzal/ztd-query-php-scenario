ARG PHP_VERSION=8.3
FROM php:${PHP_VERSION}-cli

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    unzip \
    libpq-dev \
    libsqlite3-dev \
    && docker-php-ext-install \
        pdo_mysql \
        pdo_pgsql \
        pdo_sqlite \
        mysqli \
    && rm -rf /var/lib/apt/lists/*

# Install Composer
RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer

WORKDIR /app

# Copy project files first
COPY . .

# Resolve dependencies for this PHP version (no lock file).
# Removes lock file so Composer resolves versions compatible with
# the running PHP. testcontainers-php requires symfony/process
# which may require newer PHP; we exclude it in container context
# since containers connect to DB services directly, not via Docker.
RUN rm -f composer.lock && rm -rf vendor \
    && composer remove --dev k-kinzal/testcontainers-php --no-update --no-interaction \
    && composer update --no-interaction --no-progress --prefer-dist

# Default: run all tests
ENTRYPOINT ["php", "vendor/bin/phpunit"]
CMD ["--testsuite", "Scenario"]
