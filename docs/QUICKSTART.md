# Quick Start Guide

Get up and running with the HetuEngine connector in 5 minutes.

## Prerequisites

- Apache Superset 2.0.0+
- Java 11+
- HetuEngine JDBC driver JAR file

## Installation

### 1. Install the Connector

```bash
pip install superset-hetuengine-connector
```

### 2. Verify Java Installation

```bash
java -version
# Should show Java 11 or higher
```

If Java is not installed:

```bash
# Ubuntu/Debian
sudo apt-get install openjdk-11-jre-headless

# macOS
brew install openjdk@11

# Verify installation
java -version
```

### 3. Get the JDBC Driver

Download the HetuEngine JDBC driver and place it in an accessible location:

```bash
sudo mkdir -p /opt/jdbc-drivers
sudo cp hetuengine-jdbc.jar /opt/jdbc-drivers/
sudo chmod 644 /opt/jdbc-drivers/hetuengine-jdbc.jar
```

## Configuration

### 1. Add Database in Superset

1. Log into Superset
2. Go to **Data** → **Databases**
3. Click **+ Database**
4. Select **HetuEngine** from the dropdown

### 2. Configure Connection

**SQLAlchemy URI:**
```
hetuengine://your_username:your_password@hetuengine-host:29860/hive/default
```

Replace:
- `your_username` - Your HetuEngine username
- `your_password` - Your HetuEngine password
- `hetuengine-host` - Your HetuEngine server hostname or IP
- `29860` - Port (use your actual port if different)
- `hive` - Catalog name
- `default` - Schema name

### 3. Configure Advanced Settings

Click **Advanced** → **Other** → **Engine Parameters**:

```json
{
  "connect_args": {
    "jar_path": "/opt/jdbc-drivers/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default"
  }
}
```

### 4. Test Connection

Click **Test Connection** to verify everything works.

If successful, click **Connect** to save.

## First Query

1. Go to **SQL Lab** → **SQL Editor**
2. Select your HetuEngine database
3. Run a test query:

```sql
-- Test connectivity
SELECT 1;

-- List schemas
SHOW SCHEMAS;

-- List tables
SHOW TABLES FROM default;

-- Query data
SELECT * FROM your_table LIMIT 10;
```

## Common Issues

### "JDBC driver not found"

**Problem:** JAR path is incorrect or file doesn't exist.

**Solution:**
```bash
# Verify file exists
ls -l /opt/jdbc-drivers/hetuengine-jdbc.jar

# Fix permissions if needed
chmod 644 /opt/jdbc-drivers/hetuengine-jdbc.jar
```

### "JVMNotFoundException"

**Problem:** Java is not installed or `JAVA_HOME` not set.

**Solution:**
```bash
# Install Java
sudo apt-get install openjdk-11-jre-headless

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Add to ~/.bashrc for persistence
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
```

### "404 Not Found" or "Connection failed"

**Problem:** Missing HetuEngine-specific parameters.

**Solution:** Ensure Engine Parameters includes:
```json
{
  "connect_args": {
    "jar_path": "/opt/jdbc-drivers/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default"
  }
}
```

## Docker Quick Start

### 1. Create Dockerfile

```dockerfile
FROM apache/superset:latest

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install connector
RUN pip install superset-hetuengine-connector

# Copy JDBC driver
COPY hetuengine-jdbc.jar /opt/hetuengine-jdbc.jar
RUN chmod 644 /opt/hetuengine-jdbc.jar

USER superset
```

### 2. Build and Run

```bash
# Build image
docker build -t superset-hetuengine .

# Run container
docker run -d -p 8088:8088 --name superset superset-hetuengine

# Initialize Superset (first time only)
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin
docker exec -it superset superset init
```

### 3. Access Superset

Open browser to http://localhost:8088

Default credentials (if you created admin):
- Username: admin
- Password: (what you set)

## Next Steps

- Read the [Configuration Guide](docs/configuration.md) for advanced settings
- See [Connection Examples](examples/connection_examples.md) for more use cases
- Check [Troubleshooting Guide](docs/troubleshooting.md) if you encounter issues
- Explore the [full README](README.md) for complete documentation

## Example Configurations

### Basic Connection

```
URI: hetuengine://admin:password@192.168.1.100:29860/hive/default

Engine Parameters:
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar"
  }
}
```

### SSL Connection

```
URI: hetuengine://admin:password@secure-host:29860/hive/default

Engine Parameters:
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

### Multiple Hosts

```
URI: hetuengine://admin:password@host1,host2,host3:29860/hive/default

Engine Parameters:
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default"
  }
}
```

## Testing Connection from Python

```python
from superset_hetuengine.utils import test_jdbc_connection

success, error = test_jdbc_connection(
    jar_path="/opt/hetuengine-jdbc.jar",
    host="192.168.1.100",
    port=29860,
    username="admin",
    password="password",
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

## Getting Help

- [GitHub Issues](https://github.com/pesnik/superset-hetuengine-connector/issues)
- [Full Documentation](README.md)
- [Troubleshooting Guide](docs/troubleshooting.md)

---

**Congratulations!** You now have HetuEngine connected to Superset. Start exploring your data!
