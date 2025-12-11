# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2024-01-15

### Added
- Initial release of HetuEngine connector for Apache Superset
- JDBC bridge implementation using JayDeBeAPI
- Custom SQLAlchemy dialect for HetuEngine
- Database engine specification extending Presto/Trino base
- Support for HetuEngine-specific parameters:
  - `serviceDiscoveryMode` (default: hsbroker)
  - `tenant` parameter support
  - Multiple host support for load balancing
  - SSL/TLS with configurable certificate verification
- Connection testing and validation utilities
- Schema and table introspection
- Time grain support for temporal queries
- Comprehensive error handling with user-friendly messages
- Support for HetuEngine data types
- Connection pooling support
- Extensive documentation:
  - Installation guide
  - Configuration guide
  - Troubleshooting guide
  - Connection examples
  - Docker deployment examples
- Unit tests with pytest
- Code quality tools (Black, flake8, isort, mypy)
- GitHub Actions CI/CD workflow
- Apache 2.0 license
- Contributing guidelines

### Documentation
- Comprehensive README with installation and usage instructions
- Quick start guide for getting started in 5 minutes
- Detailed configuration examples
- DBeaver to Superset configuration migration guide
- Troubleshooting guide for common issues
- Docker deployment examples and Dockerfile
- Example Superset configuration files

### Developer Experience
- Development environment setup instructions
- Testing framework with pytest
- Code formatting with Black
- Linting with flake8
- Type checking with mypy
- Import sorting with isort
- Pre-commit hook examples
- Contributing guidelines

## [0.0.1] - 2024-01-01

### Added
- Project structure
- Basic connector skeleton

---

## Release Notes

### Version 0.1.0

This is the first stable release of the HetuEngine connector for Apache Superset.

**Key Features:**

1. **JDBC Bridge:** Seamless integration with HetuEngine using Huawei's JDBC driver
2. **HetuEngine Support:** Full support for HetuEngine-specific features (serviceDiscoveryMode, tenant)
3. **High Availability:** Multiple host support for load balancing
4. **Security:** SSL/TLS support with configurable certificate verification
5. **User-Friendly:** Clear error messages and comprehensive documentation
6. **Docker Ready:** Complete Docker deployment examples

**Installation:**

```bash
pip install superset-hetuengine-connector
```

**Quick Start:**

See [../QUICKSTART.md](../QUICKSTART.md) for getting started in 5 minutes.

**Documentation:**

- [../../README.md](README.md) - Main documentation
- [docs/installation.md](docs/installation.md) - Installation guide
- [docs/configuration.md](docs/configuration.md) - Configuration guide
- [docs/troubleshooting.md](docs/troubleshooting.md) - Troubleshooting guide

**Known Limitations:**

- Requires Java 11+ to be installed
- Requires HetuEngine JDBC driver (not included)
- Currently tested with Superset 2.0.0+ and HetuEngine based on Trino

**Feedback:**

Please report issues on [GitHub Issues](https://github.com/pesnik/superset-hetuengine-connector/issues).

---

## Migration Guide

### From Development Version to 0.1.0

No migration required for new installations.

---

## Future Roadmap

Planned features for future releases:

- **0.2.0:**
  - Async query support
  - Enhanced caching strategies
  - Query result pagination
  - Additional Trino/Presto features

- **0.3.0:**
  - Advanced security features
  - Kerberos authentication support
  - LDAP integration
  - Custom UDF support

- **1.0.0:**
  - Production-ready stable release
  - Performance optimizations
  - Extended test coverage
  - Full Superset feature parity

---

## Deprecation Notices

None in this release.

---

## Security Updates

None in this release.

---

## Contributors

Thank you to all contributors who made this release possible!

- Community Contributors

To contribute, see [../CONTRIBUTING.md](../CONTRIBUTING.md).

---

[Unreleased]: https://github.com/pesnik/superset-hetuengine-connector/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/pesnik/superset-hetuengine-connector/releases/tag/v0.1.0
[0.0.1]: https://github.com/pesnik/superset-hetuengine-connector/releases/tag/v0.0.1
