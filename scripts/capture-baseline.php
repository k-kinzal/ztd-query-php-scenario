#!/usr/bin/env php
<?php

/**
 * Capture a test baseline from JUnit XML and verification-log.json.
 *
 * Usage:
 *   php scripts/capture-baseline.php [--junit build/junit.xml] [--versions spec/verification-log.json] [-o baseline.json]
 *
 * Reads JUnit XML for test results and verification-log.json for version
 * metadata. Produces baseline.json with one entry per test class:
 *
 *   {
 *     "testClass": "Tests\\Pdo\\MysqlBasicCrudTest",
 *     "tests": 5,
 *     "failures": 0,
 *     "errors": 0,
 *     "skipped": 0,
 *     "time": 1.234,
 *     "result": "pass",
 *     "phpVersion": "8.3.0",
 *     "dbVersion": "8.0.36",
 *     "ztdVersion": "0.1.1",
 *     "adapter": "mysql-pdo",
 *     "timestamp": "2026-03-08T12:00:00+00:00"
 *   }
 */

declare(strict_types=1);

$rootDir = dirname(__DIR__);

// Parse CLI options
$options = getopt('o:', ['junit:', 'versions:']);
$junitPath = $options['junit'] ?? $rootDir . '/build/junit.xml';
$versionsPath = $options['versions'] ?? $rootDir . '/spec/verification-log.json';
$outputPath = $options['o'] ?? $rootDir . '/baseline.json';

// Validate inputs
if (!file_exists($junitPath)) {
    fwrite(STDERR, "JUnit XML not found: {$junitPath}\n");
    fwrite(STDERR, "Run 'vendor/bin/phpunit' first to generate test results.\n");
    exit(1);
}

// Load version info
$versions = [];
if (file_exists($versionsPath)) {
    $content = file_get_contents($versionsPath);
    if ($content !== false) {
        $versions = json_decode($content, true) ?? [];
    }
}

// Parse JUnit XML
$xml = simplexml_load_file($junitPath);
if ($xml === false) {
    fwrite(STDERR, "Failed to parse JUnit XML: {$junitPath}\n");
    exit(1);
}

$baseline = [];
$captureTimestamp = date('c');

foreach ($xml->testsuite as $suite) {
    collectTestSuites($suite, $versions, $captureTimestamp, $baseline);
}

// Sort by test class name
usort($baseline, fn(array $a, array $b) => strcmp($a['testClass'], $b['testClass']));

// Write output
$json = json_encode([
    'capturedAt' => $captureTimestamp,
    'tests' => $baseline,
], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";

file_put_contents($outputPath, $json);

$total = count($baseline);
$pass = count(array_filter($baseline, fn(array $t) => $t['result'] === 'pass'));
$fail = $total - $pass;

fwrite(STDOUT, "Baseline captured: {$outputPath}\n");
fwrite(STDOUT, "  {$total} test classes — {$pass} pass, {$fail} with failures/errors/skips\n");

/**
 * Recursively collect leaf test suites (those with a class attribute).
 */
function collectTestSuites(SimpleXMLElement $suite, array $versions, string $timestamp, array &$baseline): void
{
    $class = (string) ($suite['name'] ?? '');

    // Leaf suite representing a test class
    if (isset($suite['file'])) {
        $tests = (int) ($suite['tests'] ?? 0);
        $failures = (int) ($suite['failures'] ?? 0);
        $errors = (int) ($suite['errors'] ?? 0);
        $skipped = (int) ($suite['skipped'] ?? 0);
        $time = (float) ($suite['time'] ?? 0);

        $result = ($failures === 0 && $errors === 0) ? 'pass' : 'fail';

        $versionInfo = $versions[$class] ?? [];

        $baseline[] = [
            'testClass' => $class,
            'tests' => $tests,
            'failures' => $failures,
            'errors' => $errors,
            'skipped' => $skipped,
            'time' => round($time, 4),
            'result' => $result,
            'phpVersion' => $versionInfo['phpVersion'] ?? PHP_VERSION,
            'dbVersion' => $versionInfo['dbVersion'] ?? 'unknown',
            'ztdVersion' => $versionInfo['ztdVersion'] ?? 'unknown',
            'adapter' => $versionInfo['adapter'] ?? 'unknown',
            'timestamp' => $versionInfo['timestamp'] ?? $timestamp,
        ];
        return;
    }

    // Recurse into nested suites
    foreach ($suite->testsuite as $child) {
        collectTestSuites($child, $versions, $timestamp, $baseline);
    }
}
