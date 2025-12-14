# Apache Superset Connector for Huawei HetuEngine

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

A database connector for Apache Superset to connect to Huawei HetuEngine (Trino-based data warehouse) using JDBC bridge.

**📖 [Complete Documentation](docs/README.md)** | **🚀 [Quick Start](docs/QUICKSTART.md)** | **📂 [Directory Structure](DIRECTORY_STRUCTURE.md)**

## Overview

HetuEngine is Huawei's enterprise data warehouse based on Trino/Presto. This connector enables Apache Superset to connect to HetuEngine using the Huawei JDBC driver via JayDeBeAPI, supporting HetuEngine-specific features like `serviceDiscoveryMode` and `tenant` parameters.

### Why This Connector?

Standard Python Trino clients don't support HetuEngine-specific connection parameters (`serviceDiscoveryMode`, `tenant`), which are required for proper connectivity. This connector bridges the gap by using Huawei's JDBC driver through JayDeBeAPI.

## Features

- Full JDBC bridge support for HetuEngine connectivity
- Support for HetuEngine-specific parameters (`serviceDiscoveryMode`, `tenant`)
- Multiple host support for load balancing
- SSL/TLS with configurable certificate verification
- Schema and table introspection
- Time grain support for temporal queries
- Comprehensive error handling with user-friendly messages
- Compatible with Docker and non-Docker Superset installations

## Prerequisites

Before installing this connector, ensure you have:

1. **Java Runtime Environment (JRE) or Java Development Kit (JDK)**
   - Java 8 or higher
   - `JAVA_HOME` environment variable set

2. **HetuEngine JDBC Driver**
   - Download the Huawei HetuEngine JDBC driver (`.jar` file)
   - Typically named `hetuengine-jdbc-<version>.jar` or similar

3. **Apache Superset**
   - Version 2.0.0 or higher

## Installation

### Option 1: Install from PyPI (once published)

Using pip:
```bash
pip install superset-hetuengine-connector
```

Using uv (recommended for faster installs):
```bash
uv pip install superset-hetuengine-connector
```

### Option 2: Install from Source

Using pip:
```bash
git clone https://github.com/pesnik/superset-hetuengine-connector.git
cd superset-hetuengine-connector
pip install -e .
```

Using uv (recommended):
```bash
git clone https://github.com/pesnik/superset-hetuengine-connector.git
cd superset-hetuengine-connector
uv sync
```

### Option 3: Install in Docker Environment

Add to your Superset Dockerfile:

```dockerfile
# Install Java
RUN apt-get update && apt-get install -y openjdk-11-jre-headless

# Install HetuEngine connector
RUN pip install superset-hetuengine-connector

# Copy JDBC driver
COPY hetuengine-jdbc.jar /opt/hetuengine-jdbc.jar
```

## Configuration

### Environment Variables (Optional)

Set these environment variables for easier configuration:

```bash
export HETUENGINE_JDBC_JAR=/path/to/hetuengine-jdbc.jar
export JAVA_HOME=/path/to/java
```

### Adding Database in Superset UI

1. Navigate to **Data** → **Databases** → **+ Database**

2. Select **HetuEngine** from the database type dropdown

3. Fill in the connection details:

   **SQLAlchemy URI:**
   ```
   hetuengine://username:password@host:port/catalog/schema
   ```

   **Example:**
   ```
   hetuengine://hetu_user:password@192.168.111.54:29860/hive/default
   ```

4. Click **Advanced** → **Other** → **Engine Parameters** and add:

   ```json
   {
     "connect_args": {
       "jar_path": "/opt/hetuengine-jdbc.jar",
       "service_discovery_mode": "hsbroker",
       "tenant": "default",
       "ssl": true,
       "ssl_verification": false
     }
   }
   ```

5. Click **Test Connection** to verify

6. Click **Connect** to save

### Connection Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `host` | Yes | - | HetuEngine server host (can be comma-separated for multiple hosts) |
| `port` | No | 29860 | HetuEngine server port |
| `username` | Yes | - | Database username |
| `password` | Yes | - | Database password |
| `catalog` | No | hive | Catalog name |
| `schema` | No | default | Schema name |
| `jar_path` | Yes | - | Path to JDBC driver JAR file |
| `service_discovery_mode` | No | hsbroker | Service discovery mode (HetuEngine-specific) |
| `tenant` | No | default | Tenant name (HetuEngine-specific) |
| `ssl` | No | false | Enable SSL/TLS |
| `ssl_verification` | No | true | Enable SSL certificate verification |

### Multiple Hosts (Load Balancing)

For high availability, you can specify multiple hosts:

```
hetuengine://user:password@192.168.111.54,192.168.111.66:29860/hive/default
```

**Engine Parameters:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default"
  }
}
```

### SSL Configuration

For SSL connections:

```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default",
    "ssl": true,
    "ssl_verification": false
  }
}
```

**Note:** Set `ssl_verification: false` if using self-signed certificates.

## DBeaver vs Superset Configuration Comparison

If you have a working connection in DBeaver, here's how to translate it to Superset:

**DBeaver JDBC URL:**
```
jdbc:trino://192.168.111.54:29860,192.168.111.66:29860/hive/default?serviceDiscoveryMode=hsbroker&tenant=default&SSL=true
```

**Equivalent Superset Configuration:**

**SQLAlchemy URI:**
```
hetuengine://username:password@192.168.111.54,192.168.111.66:29860/hive/default
```

**Engine Parameters:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default",
    "ssl": true,
    "ssl_verification": false
  }
}
```

## Troubleshooting

### Common Issues

#### 1. "JDBC driver not found"

**Error:**
```
java.lang.ClassNotFoundException: io.trino.jdbc.TrinoDriver
```

**Solution:**
- Ensure the JDBC JAR path is correct in `jar_path` parameter
- Verify the JAR file exists and is readable
- Use absolute path to JAR file

#### 2. "Java Virtual Machine not found"

**Error:**
```
JVMNotFoundException
```

**Solution:**
- Install Java (JDK or JRE)
- Set `JAVA_HOME` environment variable
- Ensure Java is in your system PATH

**Verify Java installation:**
```bash
java -version
echo $JAVA_HOME  # Linux/Mac
echo %JAVA_HOME%  # Windows
```

#### 3. "Connection refused"

**Error:**
```
Connection refused
```

**Solution:**
- Verify HetuEngine server is running
- Check host and port are correct
- Verify network connectivity (firewall, security groups)
- Test with `telnet host port`

#### 4. "404 Not Found" or Service Discovery Errors

**Error:**
```
404 Not Found
```

**Solution:**
- Ensure `service_discovery_mode` is set to `hsbroker`
- Verify `tenant` parameter is correct
- Use HetuEngine JDBC driver (not standard Trino driver)

#### 5. SSL/TLS Errors

**Error:**
```
SSL handshake failed
```

**Solution:**
- For self-signed certificates, set `ssl_verification: false`
- Verify SSL is enabled on HetuEngine server
- Check certificate validity

### Enable Debug Logging

To enable debug logging in Superset:

1. Edit `superset_config.py`:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('superset_hetuengine')
logger.setLevel(logging.DEBUG)
```

2. Restart Superset

3. Check logs for detailed error messages

### Testing Connection Outside Superset

You can test the connection using Python:

```python
from superset_hetuengine.utils import test_jdbc_connection

success, error = test_jdbc_connection(
    jar_path="/opt/hetuengine-jdbc.jar",
    host="192.168.111.54",
    port=29860,
    username="hetu_user",
    password="password",
    catalog="hive",
    schema="default",
    service_discovery_mode="hsbroker",
    tenant="default",
    ssl=True,
    ssl_verification=False,
)

if success:
    print("Connection successful!")
else:
    print(f"Connection failed: {error}")
```

## Development

### Setting Up Development Environment

Using uv (recommended):
```bash
# Clone repository
git clone https://github.com/pesnik/superset-hetuengine-connector.git
cd superset-hetuengine-connector

# Install with dev dependencies (uv automatically creates and manages venv)
uv sync --all-extras
```

Using pip:
```bash
# Clone repository
git clone https://github.com/pesnik/superset-hetuengine-connector.git
cd superset-hetuengine-connector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate  # Windows

# Install in development mode with dev dependencies
pip install -e ".[dev]"
```

### Running Tests

Using uv:
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=superset_hetuengine --cov-report=html

# Run specific test file
uv run pytest tests/test_engine_spec.py
```

Using pip:
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=superset_hetuengine --cov-report=html

# Run specific test file
pytest tests/test_engine_spec.py
```

### Code Formatting

Using uv:
```bash
# Format code with Black
uv run black superset_hetuengine tests

# Sort imports
uv run isort superset_hetuengine tests

# Lint with flake8
uv run flake8 superset_hetuengine tests

# Type checking with mypy
uv run mypy superset_hetuengine
```

Using pip:
```bash
# Format code with Black
black superset_hetuengine tests

# Sort imports
isort superset_hetuengine tests

# Lint with flake8
flake8 superset_hetuengine tests

# Type checking with mypy
mypy superset_hetuengine
```

## Docker Deployment

See [examples/docker/Dockerfile](examples/docker/Dockerfile) for a complete Docker example.

### Quick Docker Setup

```dockerfile
FROM apache/superset:latest

USER root

# Install Java
RUN apt-get update && apt-get install -y openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Install HetuEngine connector
RUN pip install superset-hetuengine-connector

# Copy JDBC driver
COPY hetuengine-jdbc.jar /opt/hetuengine-jdbc.jar
RUN chmod 644 /opt/hetuengine-jdbc.jar

USER superset
```

Build and run:

```bash
docker build -t superset-hetuengine .
docker run -d -p 8088:8088 --name superset superset-hetuengine
```

## Documentation

### 📚 Complete Documentation

- 🚀 [Quick Start Guide](docs/QUICKSTART.md) - Get started in 5 minutes
- 📦 [Installation Guide](docs/installation.md) - Detailed installation instructions
- ⚙️ [Configuration Guide](docs/configuration.md) - Complete configuration reference
- 🔧 [Troubleshooting Guide](docs/troubleshooting.md) - Common issues and solutions
- 💡 [Connection Examples](examples/connection_examples.md) - Real-world configurations
- 📋 [Changelog](docs/project/CHANGELOG.md) - Version history

See [docs/README.md](docs/README.md) for the complete documentation index.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](docs/CONTRIBUTING.md) for guidelines.

### Reporting Issues

Please report issues on [GitHub Issues](https://github.com/pesnik/superset-hetuengine-connector/issues).

Include:
- Superset version
- HetuEngine version
- Python version
- Java version
- Error messages and stack traces
- Connection configuration (redact sensitive info)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Superset team for the excellent data visualization platform
- Huawei for HetuEngine
- Trino/Presto community for the foundation

## Links

- [Apache Superset](https://superset.apache.org/)
- [Huawei HetuEngine Documentation](https://support.huawei.com/)
- [JayDeBeAPI](https://github.com/baztian/jaydebeapi)

## FAQ

### Q: Can I use the standard Trino JDBC driver?

**A:** No, you must use the Huawei HetuEngine JDBC driver. The standard Trino driver doesn't support HetuEngine-specific parameters like `serviceDiscoveryMode` and `tenant`.

### Q: Does this work with Trino or PrestoSQL?

**A:** This connector is specifically designed for Huawei HetuEngine. For standard Trino/Presto, use the built-in Superset connectors.

### Q: Can I use this without Docker?

**A:** Yes, you can install this connector in any Superset installation (Docker or non-Docker). Just ensure Java and the JDBC driver are properly configured.

### Q: How do I get the HetuEngine JDBC driver?

**A:** Contact your Huawei representative or download from Huawei's official channels. The driver is typically distributed with HetuEngine installations.

### Q: Is this connector production-ready?

**A:** This connector is in beta. Please test thoroughly in your environment before production use. Community feedback and contributions are welcome!

---

**Made with ❤️ for the data community**
