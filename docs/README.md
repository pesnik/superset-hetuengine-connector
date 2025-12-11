# Documentation Index

Welcome to the superset-hetuengine-connector documentation!

## Quick Links

### Getting Started
- 🚀 [Quick Start Guide](QUICKSTART.md) - Get up and running in 5 minutes
- 📦 [Installation Guide](installation.md) - Detailed installation instructions
- ⚙️ [Configuration Guide](configuration.md) - Complete configuration reference

### Troubleshooting & Support
- 🔧 [Troubleshooting Guide](troubleshooting.md) - Common issues and solutions
- 💡 [Connection Examples](../examples/connection_examples.md) - Real-world configuration examples

### Contributing
- 🤝 [Contributing Guide](CONTRIBUTING.md) - How to contribute to the project
- 📋 [Changelog](project/CHANGELOG.md) - Version history and release notes

### Project Information
- 📊 [Project Summary](project/PROJECT_SUMMARY.md) - Project overview and statistics
- 🗂️ [Project Structure](project/PROJECT_STRUCTURE.md) - Code organization and architecture

## Documentation Structure

```
docs/
├── README.md                    # This file - Documentation index
├── QUICKSTART.md               # 5-minute getting started guide
├── CONTRIBUTING.md             # Contribution guidelines
├── installation.md             # Installation instructions
├── configuration.md            # Configuration reference
├── troubleshooting.md          # Troubleshooting guide
└── project/
    ├── CHANGELOG.md            # Version history
    ├── PROJECT_SUMMARY.md      # Project overview
    └── PROJECT_STRUCTURE.md    # Code organization
```

## By Topic

### Installation & Setup
1. [Prerequisites](installation.md#prerequisites)
2. [Installation Methods](installation.md#installation-methods)
3. [Docker Installation](installation.md#docker-installation)
4. [Verification](installation.md#verify-installation)
5. [Post-Installation](installation.md#post-installation)

### Configuration
1. [Connection Parameters](configuration.md#connection-parameters)
2. [Superset UI Configuration](configuration.md#configuration-in-superset-ui)
3. [Environment Variables](configuration.md#environment-variables)
4. [Docker Configuration](configuration.md#docker-configuration)
5. [Security Best Practices](configuration.md#security-best-practices)
6. [Connection Pooling](configuration.md#connection-pooling)

### Troubleshooting
1. [Connection Issues](troubleshooting.md#connection-issues)
2. [Java/JVM Issues](troubleshooting.md#javajvm-issues)
3. [JDBC Driver Issues](troubleshooting.md#jdbc-driver-issues)
4. [SSL/TLS Issues](troubleshooting.md#ssltls-issues)
5. [Performance Issues](troubleshooting.md#performance-issues)
6. [Docker-Specific Issues](troubleshooting.md#docker-specific-issues)

### Examples
1. [Basic Connection](../examples/connection_examples.md#basic-connection)
2. [Multiple Hosts](../examples/connection_examples.md#multiple-hosts)
3. [SSL Connection](../examples/connection_examples.md#ssl-connection)
4. [Custom Tenant](../examples/connection_examples.md#custom-tenant)
5. [Programmatic Configuration](../examples/connection_examples.md#programmatic-configuration)
6. [Docker Setup](../examples/docker/)

## FAQ

### Installation
- **Q: Can I use this without Docker?**
  - A: Yes! See [Installation Guide](installation.md) for non-Docker installation.

- **Q: What Java version do I need?**
  - A: Java 11 or higher. See [Prerequisites](installation.md#prerequisites).

- **Q: Where do I get the JDBC driver?**
  - A: Contact Huawei support or check your HetuEngine installation.

### Configuration
- **Q: How do I configure multiple hosts?**
  - A: Use comma-separated hosts in the URI. See [Multiple Hosts](../examples/connection_examples.md#multiple-hosts).

- **Q: How do I handle self-signed SSL certificates?**
  - A: Set `ssl_verification: false`. See [SSL Configuration](configuration.md#ssl-configuration).

- **Q: Can I use environment variables for configuration?**
  - A: Yes! See [Environment Variables](configuration.md#environment-variables).

### Troubleshooting
- **Q: Getting "JDBC driver not found" error?**
  - A: Check JAR path is correct. See [JDBC Driver Issues](troubleshooting.md#jdbc-driver-issues).

- **Q: Getting "JVMNotFoundException"?**
  - A: Java is not installed or JAVA_HOME not set. See [Java/JVM Issues](troubleshooting.md#javajvm-issues).

- **Q: Getting "404 Not Found" error?**
  - A: Missing HetuEngine-specific parameters. See [Connection Issues](troubleshooting.md#error-404-not-found--service-discovery-error).

## Support & Resources

- **GitHub Issues**: https://github.com/pesnik/superset-hetuengine-connector/issues
- **Main README**: [../README.md](../README.md)
- **Examples**: [../examples/](../examples/)

## Version Information

Current version: **0.1.0** (Beta)

See [CHANGELOG.md](project/CHANGELOG.md) for version history.

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- How to report bugs
- How to suggest features
- Development setup
- Coding standards
- Pull request process

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) for details.

---

**Need help?** Check the [Troubleshooting Guide](troubleshooting.md) or [open an issue](https://github.com/pesnik/superset-hetuengine-connector/issues).
