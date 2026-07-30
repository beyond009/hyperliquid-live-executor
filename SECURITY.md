# Security

This service can submit real Hyperliquid orders. Use a dedicated API wallet with the minimum
permissions required. Never use a master-account private key.

Private keys must only be stored with the built-in encrypted key vault. Do not commit `.env`,
database, log, wallet, seed phrase, or encrypted-key files. Mainnet mode requires explicit runtime
acknowledgement and a non-example risk policy.

Report vulnerabilities privately to the repository owner. Do not open a public issue containing
credentials, account details, order payloads, or exploit instructions.
