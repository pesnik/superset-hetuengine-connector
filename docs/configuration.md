# Configuration Guide

This guide covers configuring HetuEngine database connections in Apache Superset.

## Connection Parameters

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `host` | HetuEngine server hostname or IP | `192.168.111.54` |
| `port` | HetuEngine server port | `29860` |
| `username` | Database username | `hetu_user` |
| `password` | Database password | `********` |
| `jar_path` | Path to JDBC driver JAR | `/opt/hetuengine-jdbc.jar` |

### Optional Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `catalog` | `hive` | Catalog name |
| `schema` | `default` | Schema name |
| `service_discovery_mode` | `hsbroker` | Service discovery mode |
| `tenant` | `default` | Tenant name |
| `ssl` | `false` | Enable SSL/TLS |
| `ssl_verification` | `true` | Verify SSL certificates |

## Configuration in Superset UI

### Step 1: Add Database

1. Log in to Superset
2. Navigate to **Data** → **Databases**
3. Click **+ Database** button
4. Select **HetuEngine** from the dropdown

### Step 2: Configure Connection

**SQLAlchemy URI Format:**
```
hetuengine://username:password@host:port/catalog/schema
```

**Example:**
```
hetuengine://hetu_user:mypassword@192.168.111.54:29860/hive/default
```

### Step 3: Configure Advanced Settings

Click **Advanced** → **Other** → **Engine Parameters**:

```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default",
    "ssl": false,
    "ssl_verification": true
  }
}
```

### Step 4: Test Connection

1. Click **Test Connection**
2. Verify success message
3. Click **Connect** to save

## Configuration Examples

### Basic Connection

**SQLAlchemy URI:**
```
hetuengine://admin:password@192.168.1.100:29860/hive/default
```

**Engine Parameters:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar"
  }
}
```

### Multiple Hosts (Load Balancing)

**SQLAlchemy URI:**
```
hetuengine://admin:password@host1,host2,host3:29860/hive/default
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

### SSL Connection

**SQLAlchemy URI:**
```
hetuengine://admin:password@secure-host:29860/hive/default
```

**Engine Parameters:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default",
    "ssl": true,
    "ssl_verification": true
  }
}
```

### SSL with Self-Signed Certificate

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

### Custom Tenant

**SQLAlchemy URI:**
```
hetuengine://admin:password@192.168.1.100:29860/hive/my_schema
```

**Engine Parameters:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "custom_tenant"
  }
}
```

## Environment Variables

You can set default values using environment variables:

```bash
# JDBC JAR path
export HETUENGINE_JDBC_JAR=/opt/hetuengine-jdbc.jar

# Java home
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Default connection parameters (optional)
export HETUENGINE_HOST=192.168.1.100
export HETUENGINE_PORT=29860
export HETUENGINE_CATALOG=hive
export HETUENGINE_SCHEMA=default
export HETUENGINE_TENANT=default
```

When using environment variables, you can omit `jar_path` from Engine Parameters:

**Engine Parameters:**
```json
{
  "connect_args": {
    "service_discovery_mode": "hsbroker",
    "tenant": "default"
  }
}
```

## Docker Configuration

### Environment Variables in Docker

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  superset:
    image: superset-hetuengine:latest
    environment:
      - JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
      - HETUENGINE_JDBC_JAR=/opt/hetuengine-jdbc.jar
      - HETUENGINE_HOST=hetuengine-server
      - HETUENGINE_PORT=29860
    volumes:
      - ./jdbc-drivers:/opt/jdbc-drivers
```

### Mounting JDBC Driver

**docker-compose.yml:**
```yaml
services:
  superset:
    image: superset-hetuengine:latest
    volumes:
      - ./hetuengine-jdbc.jar:/opt/hetuengine-jdbc.jar:ro
```

## Programmatic Configuration

### Python Configuration

You can configure connections programmatically:

```python
from superset import db
from superset.models.core import Database

# Create database connection
database = Database(
    database_name="HetuEngine Production",
    sqlalchemy_uri="hetuengine://admin:password@hetuengine-prod:29860/hive/default",
    extra=json.dumps({
        "connect_args": {
            "jar_path": "/opt/hetuengine-jdbc.jar",
            "service_discovery_mode": "hsbroker",
            "tenant": "production",
            "ssl": True,
            "ssl_verification": False
        }
    })
)

db.session.add(database)
db.session.commit()
```

### superset_config.py

Add default database in `superset_config.py`:

```python
import os

# HetuEngine connection defaults
HETUENGINE_CONFIG = {
    "jar_path": os.environ.get("HETUENGINE_JDBC_JAR", "/opt/hetuengine-jdbc.jar"),
    "service_discovery_mode": "hsbroker",
    "tenant": os.environ.get("HETUENGINE_TENANT", "default"),
}

# Pre-configured databases
SQLALCHEMY_EXAMPLES_URI = (
    f"hetuengine://admin:password@hetuengine-server:29860/hive/default"
)
```

## Security Best Practices

### 1. Use Encrypted Credentials

Store sensitive parameters in `encrypted_extra` instead of `extra`:

```python
database = Database(
    database_name="HetuEngine",
    sqlalchemy_uri="hetuengine://admin@hetuengine:29860/hive/default",
    encrypted_extra=json.dumps({
        "connect_args": {
            "jar_path": "/opt/hetuengine-jdbc.jar",
            "tenant": "secure_tenant"
        }
    })
)
```

### 2. Use Environment Variables for Secrets

```bash
# Don't hardcode passwords
export HETUENGINE_PASSWORD=secure_password
```

```python
import os

password = os.environ.get("HETUENGINE_PASSWORD")
uri = f"hetuengine://admin:{password}@host:29860/hive/default"
```

### 3. Restrict File Permissions

```bash
# Secure JDBC driver
chmod 644 /opt/hetuengine-jdbc.jar
chown superset:superset /opt/hetuengine-jdbc.jar

# Secure Superset config
chmod 600 superset_config.py
```

### 4. Use SSL/TLS

Always enable SSL for production:

```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "ssl": true,
    "ssl_verification": true
  }
}
```

## Connection Pooling

Configure connection pooling for better performance:

**Engine Parameters:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default"
  },
  "engine_params": {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 3600
  }
}
```

## Troubleshooting Configuration

### Test Connection from Python

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
    ssl=False,
)

print(f"Success: {success}")
if error:
    print(f"Error: {error}")
```

### Verify Java Configuration

```python
from superset_hetuengine.utils import check_java_installation

is_installed, version = check_java_installation()
print(f"Java installed: {is_installed}")
print(f"Java version: {version}")
```

### Verify JDBC JAR

```python
from superset_hetuengine.utils import validate_jdbc_jar

is_valid, error = validate_jdbc_jar("/opt/hetuengine-jdbc.jar")
print(f"JAR valid: {is_valid}")
if error:
    print(f"Error: {error}")
```

### Enable Debug Logging

In `superset_config.py`:

```python
import logging

# Enable debug logging
LOG_LEVEL = logging.DEBUG

# Configure HetuEngine logger
logging.getLogger('superset_hetuengine').setLevel(logging.DEBUG)
```

## Migration from DBeaver

If you have a working DBeaver connection, translate it as follows:

**DBeaver JDBC URL:**
```
jdbc:trino://192.168.111.54:29860,192.168.111.66:29860/hive/default?serviceDiscoveryMode=hsbroker&tenant=production&SSL=true
```

**Superset Configuration:**

**SQLAlchemy URI:**
```
hetuengine://your_username:your_password@192.168.111.54,192.168.111.66:29860/hive/default
```

**Engine Parameters:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "production",
    "ssl": true,
    "ssl_verification": false
  }
}
```

## Next Steps

- Review [Troubleshooting Guide](troubleshooting.md) for common issues
- See main [README](../README.md) for usage examples
- Check [Installation Guide](installation.md) for setup instructions
