# Security Policy

## Supported versions

Only the `main` branch receives security fixes.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability** (or go to <https://github.com/Pop-Iuliu/SomniumDB/security/advisories/new>). Do not open a public issue.

Include what you found, how to reproduce it, and the commit you tested.

## What to expect

You will get an acknowledgement within 7 days. There is no fixed timeline for the fix beyond that; we will keep you updated in the advisory.

## Scope

What SomniumDB protects, and what it assumes, is described in the threat model in [`securityprogress.md`](securityprogress.md). In short: every client and peer that knows the password is fully trusted, and traffic is not encrypted by the server itself. Reports about behaviour outside that model are still welcome, but may be treated as hardening rather than as vulnerabilities.
