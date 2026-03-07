# AGENTS

This file is for agents to understand the context of the project.

## Project Goal

The objective of this project is to utilize k-kinzal/ztd-query-mysqli-adapter and k-kinzal/ztd-query-pdo-adapter from a user perspective.
Let's identify potential problems in advance to proactively resolve the issues that users of ztd-query-php might encounter.

## Something important

- Have you covered all the patterns that ztd-query-php satisfies?
- Does it comprehensively cover everything that ztd-query-php supports?
- Is it designed to be compatible with new versions when they are released?

## What You Should Do

- Create user scenarios using PHPUnit.
- Document the specifications identified through those scenarios in the "spec" directory using EARS (Easy Approach to Requirements Syntax) notation.
   - Periodically review the specs to ensure there are no contradictions or omissions.
- Clearly specify the version of ztd-query for both adapters.
- Always keep up with the latest version of ztd-query.
- Spawn an Architect and a Critic as your teammates to provide you with advice and consultation.

## When You Find an Issue

- Definition of an "Issue":
   - Unable to achieve the expected behavior.
   - High effort to use or poor usability.
      - Please note that ztd-query aims for "SIMPLE," not "EASY." Keep this distinction in mind.
- Report issues at https://github.com/k-kinzal/ztd-query-php/issues.
- Research past issues before reporting to avoid duplicates.
- Note that you should report "issues," not "proposals."
- Issues must be clear and reproducible.

## Tech Stack

- PHP 8.1 - 8.5
- MySQL 5.6 - 9.1
- PostgreSQL 14 - 18
- SQLite 3
- k-kinzal/ztd-query-mysqli-adapter
- k-kinzal/ztd-query-pdo-adapter
- k-kinzal/testcontainers-php
