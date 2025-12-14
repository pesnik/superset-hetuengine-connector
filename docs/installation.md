# Installation Guide

This guide walks through installing the HetuEngine connector for Apache Superset.

## Prerequisites

Before installing the connector, ensure you have the following:

### 1. Java Runtime Environment

HetuEngine connector requires Java to run JDBC connections.

**Install Java (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install -y openjdk-11-jre-headless
```

**Install Java (CentOS/RHEL):**
```bash
sudo yum install -y java-11-openjdk
```

**Install Java (macOS):**
```bash
brew install openjdk@11
```

**Verify Installation:**
```bash
java -version
```

**Set JAVA_HOME:**
```bash
# Linux/macOS (add to ~/.bashrc or ~/.zshrc)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Verify
echo $JAVA_HOME
```

### 2. HetuEngine JDBC Driver

Download the HetuEngine JDBC driver from Huawei or your HetuEngine installation.

The driver is typically named:
- `hetuengine-jdbc-<version>.jar`
- `hetu-jdbc-<version>.jar`

**Place the driver in a accessible location:**
```bash
# Example
sudo mkdir -p /opt/jdbc-drivers
sudo cp hetuengine-jdbc.jar /opt/jdbc-drivers/
sudo chmod 644 /opt/jdbc-drivers/hetuengine-jdbc.jar
```

### 3. Apache Superset

Install Apache Superset 2.0.0 or higher.

**Via pip:**
```bash
pip install apache-superset
```

**Verify:**
```bash
superset version
```

## Installation Methods

### Method 1: Install from PyPI (Recommended)

Once the package is published to PyPI:

```bash
pip install superset-hetuengine-connector
```

### Method 2: Install from Source

**Clone the repository:**
```bash
git clone https://github.com/pesnik/superset-hetuengine-connector.git
cd superset-hetuengine-connector
```

**Install in development mode:**
```bash
pip install -e .
```

**Or install normally:**
```bash
pip install .
```

### Method 3: Install from GitHub

```bash
pip install git+https://github.com/pesnik/superset-hetuengine-connector.git
```

## Docker Installation

### Option 1: Extend Official Superset Image

Create a `Dockerfile`:

```dockerfile
FROM apache/superset:latest

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install HetuEngine connector
RUN pip install superset-hetuengine-connector

# Copy JDBC driver
COPY hetuengine-jdbc.jar /opt/hetuengine-jdbc.jar
RUN chmod 644 /opt/hetuengine-jdbc.jar

USER superset
```

**Build and run:**
```bash
docker build -t superset-hetuengine .
docker run -d -p 8088:8088 --name superset superset-hetuengine
```

### Option 2: Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  superset:
    build:
      context: .
      dockerfile: Dockerfile
    ports:
      - "8088:8088"
    environment:
      - JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
      - HETUENGINE_JDBC_JAR=/opt/hetuengine-jdbc.jar
    volumes:
      - ./superset_home:/app/superset_home
```

**Start services:**
```bash
docker-compose up -d
```

## Verify Installation

### 1. Check Python Package

```bash
python -c "import superset_hetuengine; print(superset_hetuengine.__version__)"
```

### 2. Check Java

```bash
java -version
```

### 3. Test JDBC Connection

Create a test script `test_connection.py`:

```python
from superset_hetuengine.utils import test_jdbc_connection

success, error = test_jdbc_connection(
    jar_path="/opt/hetuengine-jdbc.jar",
    host="your-hetuengine-host",
    port=29860,
    username="your-username",
    password="your-password",
    catalog="hive",
    schema="default",
    service_discovery_mode="hsbroker",
    tenant="default",
)

if success:
    print("✓ Connection successful!")
else:
    print(f"✗ Connection failed: {error}")
```

Run the test:
```bash
python test_connection.py
```

## Post-Installation

### 1. Configure Superset

Edit `superset_config.py`:

```python
# Enable HetuEngine connector
ADDITIONAL_ALLOWED_DB_ENGINES = {
    'hetuengine': {
        'SQLALCHEMY_DATABASE_URI': 'hetuengine://...'
    }
}
```

### 2. Initialize Superset Database

If this is a new Superset installation:

```bash
# Create admin user
superset fab create-admin

# Initialize database
superset db upgrade

# Load examples (optional)
superset load_examples

# Create default roles and permissions
superset init
```

### 3. Start Superset

```bash
# Development mode
superset run -p 8088 --with-threads --reload --debugger

# Production mode
gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 4 \
    --worker-class gthread \
    --threads 2 \
    --timeout 300 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
```

## Troubleshooting

### Java Not Found

**Error:**
```
JVMNotFoundException
```

**Solution:**
- Verify Java is installed: `java -version`
- Set JAVA_HOME: `export JAVA_HOME=/path/to/java`
- Add to PATH: `export PATH=$JAVA_HOME/bin:$PATH`

### JDBC Driver Not Found

**Error:**
```
java.lang.ClassNotFoundException: io.trino.jdbc.TrinoDriver
```

**Solution:**
- Verify JAR file exists: `ls -l /opt/hetuengine-jdbc.jar`
- Check file permissions: `chmod 644 /opt/hetuengine-jdbc.jar`
- Use absolute path in configuration

### Permission Denied

**Error:**
```
PermissionError: [Errno 13] Permission denied
```

**Solution:**
```bash
# Fix JAR file permissions
sudo chmod 644 /opt/hetuengine-jdbc.jar

# Fix directory permissions
sudo chmod 755 /opt
```

### Import Error

**Error:**
```
ModuleNotFoundError: No module named 'superset_hetuengine'
```

**Solution:**
- Reinstall package: `pip install --force-reinstall superset-hetuengine-connector`
- Check installation: `pip list | grep hetuengine`

## Upgrade

To upgrade to the latest version:

```bash
pip install --upgrade superset-hetuengine-connector
```

For Docker:

```bash
docker pull your-registry/superset-hetuengine:latest
docker-compose up -d
```

## Uninstallation

To remove the connector:

```bash
pip uninstall superset-hetuengine-connector
```

## Next Steps

After successful installation:

1. See [Configuration Guide](configuration.md) for setting up database connections
2. See [Troubleshooting Guide](troubleshooting.md) for common issues
3. Check the main [README](../README.md) for usage examples
