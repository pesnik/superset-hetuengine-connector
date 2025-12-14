# Connection Examples

This document provides various examples of configuring HetuEngine connections in Apache Superset.

## Table of Contents

- [Basic Connection](#basic-connection)
- [Multiple Hosts](#multiple-hosts)
- [SSL Connection](#ssl-connection)
- [Custom Tenant](#custom-tenant)
- [Connection Pooling](#connection-pooling)
- [Programmatic Configuration](#programmatic-configuration)

## Basic Connection

The simplest HetuEngine connection configuration.

**SQLAlchemy URI:**
```
hetuengine://admin:password@192.168.1.100:29860/hive/default
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

## Multiple Hosts

Configure multiple HetuEngine hosts for load balancing and high availability.

**SQLAlchemy URI:**
```
hetuengine://admin:password@host1.example.com,host2.example.com,host3.example.com:29860/hive/default
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

**With IP addresses:**
```
hetuengine://admin:password@192.168.111.54,192.168.111.66,192.168.111.78:29860/hive/default
```

## SSL Connection

### Basic SSL

**SQLAlchemy URI:**
```
hetuengine://admin:password@secure-hetuengine.example.com:29860/hive/default
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

## Custom Tenant

Configure a custom tenant for multi-tenant HetuEngine deployments.

**SQLAlchemy URI:**
```
hetuengine://tenant_user:password@hetuengine.example.com:29860/hive/tenant_schema
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

## Connection Pooling

Configure connection pooling for better performance.

**SQLAlchemy URI:**
```
hetuengine://admin:password@hetuengine.example.com:29860/hive/default
```

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
    "pool_recycle": 3600,
    "pool_pre_ping": true
  }
}
```

**Explanation:**
- `pool_size`: Number of connections to maintain in the pool
- `max_overflow`: Maximum number of connections that can be created beyond pool_size
- `pool_timeout`: Seconds to wait before giving up on getting a connection
- `pool_recycle`: Seconds after which to recycle connections
- `pool_pre_ping`: Test connections before using them

## Programmatic Configuration

### Using Python API

```python
import json
from superset import db
from superset.models.core import Database

# Create HetuEngine database connection
hetuengine_db = Database(
    database_name="HetuEngine Production",
    sqlalchemy_uri="hetuengine://admin:password@hetuengine-prod:29860/hive/default",
    extra=json.dumps({
        "connect_args": {
            "jar_path": "/opt/hetuengine-jdbc.jar",
            "service_discovery_mode": "hsbroker",
            "tenant": "production",
            "ssl": True,
            "ssl_verification": False
        },
        "engine_params": {
            "pool_size": 15,
            "max_overflow": 30,
            "pool_recycle": 3600
        }
    })
)

# Add to database
db.session.add(hetuengine_db)
db.session.commit()

print(f"Database '{hetuengine_db.database_name}' created successfully!")
```

### Using Superset CLI

Create a Python script `add_hetuengine_db.py`:

```python
#!/usr/bin/env python3
import json
from superset import app, db
from superset.models.core import Database

def add_hetuengine_database():
    with app.app_context():
        # Check if database already exists
        existing_db = db.session.query(Database).filter_by(
            database_name="HetuEngine Production"
        ).first()

        if existing_db:
            print("Database already exists!")
            return

        # Create new database
        hetuengine_db = Database(
            database_name="HetuEngine Production",
            sqlalchemy_uri="hetuengine://admin:password@hetuengine:29860/hive/default",
            extra=json.dumps({
                "connect_args": {
                    "jar_path": "/opt/hetuengine-jdbc.jar",
                    "service_discovery_mode": "hsbroker",
                    "tenant": "production"
                }
            })
        )

        db.session.add(hetuengine_db)
        db.session.commit()

        print(f"Database '{hetuengine_db.database_name}' created successfully!")

if __name__ == "__main__":
    add_hetuengine_database()
```

Run the script:
```bash
FLASK_APP=superset python add_hetuengine_db.py
```

### Using Environment Variables

```python
import os
import json
from superset import db
from superset.models.core import Database

# Read configuration from environment
hetuengine_db = Database(
    database_name=os.environ.get("HETUENGINE_DB_NAME", "HetuEngine"),
    sqlalchemy_uri=(
        f"hetuengine://{os.environ.get('HETUENGINE_USER')}:"
        f"{os.environ.get('HETUENGINE_PASSWORD')}@"
        f"{os.environ.get('HETUENGINE_HOST')}:"
        f"{os.environ.get('HETUENGINE_PORT', '29860')}/"
        f"{os.environ.get('HETUENGINE_CATALOG', 'hive')}/"
        f"{os.environ.get('HETUENGINE_SCHEMA', 'default')}"
    ),
    extra=json.dumps({
        "connect_args": {
            "jar_path": os.environ.get('HETUENGINE_JDBC_JAR'),
            "service_discovery_mode": os.environ.get('HETUENGINE_SDM', 'hsbroker'),
            "tenant": os.environ.get('HETUENGINE_TENANT', 'default'),
        }
    })
)

db.session.add(hetuengine_db)
db.session.commit()
```

Set environment variables:
```bash
export HETUENGINE_DB_NAME="HetuEngine Production"
export HETUENGINE_USER="admin"
export HETUENGINE_PASSWORD="password"
export HETUENGINE_HOST="hetuengine-prod.example.com"
export HETUENGINE_PORT="29860"
export HETUENGINE_CATALOG="hive"
export HETUENGINE_SCHEMA="default"
export HETUENGINE_JDBC_JAR="/opt/hetuengine-jdbc.jar"
export HETUENGINE_SDM="hsbroker"
export HETUENGINE_TENANT="production"
```

## Testing Connection

### Python Test Script

```python
from superset_hetuengine.utils import test_jdbc_connection

# Test basic connection
success, error = test_jdbc_connection(
    jar_path="/opt/hetuengine-jdbc.jar",
    host="hetuengine.example.com",
    port=29860,
    username="admin",
    password="password",
    catalog="hive",
    schema="default",
    service_discovery_mode="hsbroker",
    tenant="default",
    ssl=False,
)

if success:
    print("✓ Connection successful!")
else:
    print(f"✗ Connection failed: {error}")
```

### SQL Lab Test

After configuring the connection in Superset UI:

1. Go to **SQL Lab** → **SQL Editor**
2. Select your HetuEngine database
3. Run a test query:

```sql
-- Test basic connectivity
SELECT 1;

-- List schemas
SHOW SCHEMAS;

-- List tables
SHOW TABLES FROM default;

-- Query sample data
SELECT * FROM default.sample_table LIMIT 10;
```

## Migration from DBeaver

If you have a working DBeaver connection, translate it as follows:

**DBeaver Configuration:**
- **JDBC URL:** `jdbc:trino://192.168.111.54:29860/hive/default?serviceDiscoveryMode=hsbroker&tenant=production&SSL=true`
- **Username:** `admin`
- **Password:** `password`
- **Driver:** HetuEngine JDBC Driver

**Superset Configuration:**

**SQLAlchemy URI:**
```
hetuengine://admin:password@192.168.111.54:29860/hive/default
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

## Docker Configuration

### docker-compose.yml

```yaml
version: '3.8'

services:
  superset:
    image: superset-hetuengine:latest
    environment:
      - HETUENGINE_JDBC_JAR=/opt/hetuengine-jdbc.jar
      - HETUENGINE_HOST=hetuengine.example.com
      - HETUENGINE_PORT=29860
      - HETUENGINE_USER=admin
      - HETUENGINE_PASSWORD=password
      - HETUENGINE_TENANT=production
    volumes:
      - ./hetuengine-jdbc.jar:/opt/hetuengine-jdbc.jar:ro
```

### Environment File

Create `.env` file:

```env
# HetuEngine Configuration
HETUENGINE_JDBC_JAR=/opt/hetuengine-jdbc.jar
HETUENGINE_HOST=hetuengine.example.com
HETUENGINE_PORT=29860
HETUENGINE_USER=admin
HETUENGINE_PASSWORD=secure_password_here
HETUENGINE_CATALOG=hive
HETUENGINE_SCHEMA=default
HETUENGINE_TENANT=production
HETUENGINE_SSL=true
HETUENGINE_SSL_VERIFY=false
```

Reference in docker-compose.yml:

```yaml
services:
  superset:
    env_file:
      - .env
```

## Advanced Examples

### Read-Only User

```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "readonly"
  },
  "allow_dml": false,
  "allow_run_async": true
}
```

### Development vs Production

**Development:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "dev",
    "ssl": false
  },
  "engine_params": {
    "pool_size": 5,
    "max_overflow": 10
  }
}
```

**Production:**
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "production",
    "ssl": true,
    "ssl_verification": true
  },
  "engine_params": {
    "pool_size": 20,
    "max_overflow": 40,
    "pool_recycle": 3600,
    "pool_pre_ping": true
  }
}
```

## Troubleshooting

If connection fails, verify:

1. **JDBC JAR path is correct**
   ```bash
   ls -l /opt/hetuengine-jdbc.jar
   ```

2. **Java is installed**
   ```bash
   java -version
   ```

3. **Network connectivity**
   ```bash
   telnet hetuengine.example.com 29860
   ```

4. **Test connection script**
   ```python
   from superset_hetuengine.utils import test_jdbc_connection
   # ... (see Python Test Script above)
   ```

For more troubleshooting, see [Troubleshooting Guide](../docs/troubleshooting.md).
