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

# The lock file is resolved for PHP 8.1, the oldest supported runtime.
# Every matrix image installs the same ZTD commits and test dependencies.
RUN composer install --no-interaction --no-progress --prefer-dist \
    && composer check-platform-reqs

# Default: run all tests
ENTRYPOINT ["php", "vendor/bin/phpunit"]
CMD ["--testsuite", "Scenario"]
