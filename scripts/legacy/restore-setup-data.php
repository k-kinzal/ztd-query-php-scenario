#!/usr/bin/env php
<?php

/**
 * Restore setUp() INSERT data that was lost during the refactoring at commit 18a1b66.
 *
 * For each SQLite test file that is missing setUp() method but had one in the
 * pre-refactoring commit, this script extracts the INSERT statements and
 * generates a setUp() method using the new base class pattern.
 */

declare(strict_types=1);

$rootDir = dirname(__DIR__);
$testsDir = $rootDir . '/tests/Pdo';
$preRefactorCommit = '18a1b66~1';

// Find all affected files
$files = glob($testsDir . '/Sqlite*.php');
$fixed = 0;
$skipped = 0;

foreach ($files as $file) {
    $class = basename($file, '.php');
    $relPath = 'tests/Pdo/' . basename($file);

    // Read current file
    $current = file_get_contents($file);

    // Skip if already has setUp with INSERTs
    if (preg_match('/function setUp\(\).*?\n\s*\{.*?INSERT/s', $current)) {
        continue;
    }

    // Get pre-refactoring version from git
    $original = shell_exec("git show {$preRefactorCommit}:{$relPath} 2>/dev/null");
    if ($original === null || $original === '') {
        continue;
    }

    // Extract setUp method from original
    if (!preg_match('/protected function setUp\(\): void\s*\{(.*?)\n    \}/s', $original, $match)) {
        continue;
    }

    $originalSetup = $match[1];

    // Extract INSERT statements from original setUp
    $insertLines = [];
    foreach (explode("\n", $originalSetup) as $line) {
        $trimmed = trim($line);
        if (stripos($trimmed, 'INSERT') !== false || stripos($trimmed, 'insert') !== false) {
            // Adapt: $this->pdo->exec() for PDO tests
            if (strpos($trimmed, '$this->pdo->exec(') !== false ||
                strpos($trimmed, '$this->pdo->query(') !== false) {
                $insertLines[] = '        ' . $trimmed;
            }
        }
    }

    if (empty($insertLines)) {
        continue;
    }

    // Check if file already has a setUp method
    $hasSetup = strpos($current, 'function setUp()') !== false;

    if ($hasSetup) {
        // Add INSERTs after parent::setUp() in existing setUp
        $insertsBlock = implode("\n", $insertLines);
        $current = preg_replace(
            '/(parent::setUp\(\);)\s*\n(\s*\})/',
            "$1\n\n{$insertsBlock}\n$2",
            $current,
            1,
            $count
        );
        if ($count === 0) {
            // Try adding before closing brace of setUp
            $current = preg_replace(
                '/(function setUp\(\): void\s*\{[^}]*?parent::setUp\(\);)(\s*\})/s',
                "$1\n\n{$insertsBlock}\n    }",
                $current,
                1
            );
        }
    } else {
        // Create new setUp method
        $insertsBlock = implode("\n", $insertLines);
        $setUpMethod = <<<PHP

    protected function setUp(): void
    {
        parent::setUp();

{$insertsBlock}
    }

PHP;

        // Insert setUp after getTableNames method
        if (preg_match('/(\n    protected function getTableNames\(\): array\s*\{[^}]*\})\s*\n/', $current, $m, PREG_OFFSET_CAPTURE)) {
            $insertPos = $m[0][1] + strlen($m[0][0]);
            $current = substr($current, 0, $insertPos) . $setUpMethod . substr($current, $insertPos);
        } else {
            // Fallback: insert before first test method
            $current = preg_replace(
                '/(\n    public function test)/',
                $setUpMethod . "\n    public function test",
                $current,
                1
            );
        }
    }

    file_put_contents($file, $current);
    $fixed++;
    echo "FIXED: {$class}\n";
}

echo "\nDone: {$fixed} files fixed\n";
