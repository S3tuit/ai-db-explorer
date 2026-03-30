# Security Policy

## Supported Versions

Security fixes are provided on a best-effort basis for:

- the latest tagged release
- the current default branch before the next release

Older releases may not receive fixes.

## Reporting a Vulnerability

Please, do not report undisclosed vulnerabilities in public issues, pull
requests, or discussion threads.

Prefer a private reporting channel:

- You can send an email at matteo.ladislai@hotmail.com.  

Please include:

- affected version or commit
- operating system and architecture
- clear reproduction steps
- expected impact
- whether the issue can expose credentials, bypass policy, or leak sensitive
  values

Do not include live database credentials, production secrets, or private data
that you are not authorized to share.

## Response Expectations

Best effort target:

- acknowledgment within 5 business days
- follow-up once triage is complete
- coordinated disclosure after a fix is available or mitigations are agreed

## Scope Notes

This project treats the Broker as the primary security boundary. Reports are
especially useful when they involve:

- Broker policy bypass
- credential exposure
- secret-store leakage
- sandbox or IPC boundary weakness
- sensitive-token misuse or cross-session reuse
- validation failures that could allow unsafe SQL behavior

Really, thank you.
