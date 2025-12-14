# Troubleshooting Guide

This guide helps resolve common issues when using the HetuEngine connector.

## Table of Contents

- [Connection Issues](#connection-issues)
- [Java/JVM Issues](#javajvm-issues)
- [JDBC Driver Issues](#jdbc-driver-issues)
- [SSL/TLS Issues](#ssltls-issues)
- [Performance Issues](#performance-issues)
- [Query Issues](#query-issues)
- [Docker-Specific Issues](#docker-specific-issues)

## Connection Issues

### Error: Connection Refused

**Symptoms:**
```
Connection refused
Unable to connect to HetuEngine server
```

**Possible Causes & Solutions:**

1. **HetuEngine server is not running**
   ```bash
   # Check if server is reachable
   telnet 192.168.111.54 29860
   # or
   nc -zv 192.168.111.54 29860
   ```

2. **Wrong host or port**
   - Verify host and port in connection string
   - Check for typos in hostname/IP

3. **Firewall blocking connection**
   ```bash
   # Test connectivity
   curl -v telnet://192.168.111.54:29860
   ```
   - Contact network administrator to open port 29860

4. **Network routing issues**
   - Verify Superset can reach HetuEngine network
   - Check VPN/proxy settings

### Error: 404 Not Found / Service Discovery Error

**Symptoms:**
```
404 Not Found
Error: serviceDiscoveryMode not recognized
```

**Solution:**

This is the most common error with HetuEngine. Ensure you're using the correct parameters:

```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default"
  }
}
```

**Verify:**
- `service_discovery_mode` is set to `hsbroker`
- `tenant` parameter is specified
- You're using HetuEngine JDBC driver (not standard Trino driver)

### Error: Authentication Failed

**Symptoms:**
```
Authentication failed
Invalid credentials
Access denied
```

**Solutions:**

1. **Verify credentials**
   - Check username and password
   - Ensure no special characters are improperly encoded

2. **URL-encode special characters**
   ```python
   from urllib.parse import quote_plus

   username = quote_plus("user@domain")
   password = quote_plus("p@ssw0rd!")
   uri = f"hetuengine://{username}:{password}@host:29860/hive/default"
   ```

3. **Check user permissions**
   - Verify user has access to HetuEngine
   - Check tenant permissions

## Java/JVM Issues

### Error: JVMNotFoundException

**Symptoms:**
```
JVMNotFoundException
jpype._jvmfinder.JVMNotFoundException
Java Virtual Machine not found
```

**Solutions:**

1. **Install Java**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install openjdk-11-jre-headless

   # CentOS/RHEL
   sudo yum install java-11-openjdk

   # macOS
   brew install openjdk@11
   ```

2. **Set JAVA_HOME**
   ```bash
   # Find Java installation
   which java
   /usr/bin/java -XshowSettings:properties -version 2>&1 | grep java.home

   # Set JAVA_HOME
   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
   export PATH=$JAVA_HOME/bin:$PATH

   # Add to ~/.bashrc or ~/.zshrc for persistence
   echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
   ```

3. **Verify Java installation**
   ```bash
   java -version
   echo $JAVA_HOME
   ```

### Error: Java Version Incompatible

**Symptoms:**
```
UnsupportedClassVersionError
java.lang.UnsupportedClassVersionError: Bad version number
```

**Solution:**

Ensure Java 8 or higher is installed:
```bash
java -version
# Should show version 1.8.0 or higher
```

Upgrade if necessary:
```bash
sudo apt-get install openjdk-11-jre-headless
```

## JDBC Driver Issues

### Error: ClassNotFoundException

**Symptoms:**
```
java.lang.ClassNotFoundException: io.trino.jdbc.TrinoDriver
ClassNotFoundException: io.trino.jdbc.TrinoDriver
```

**Solutions:**

1. **Verify JAR file exists**
   ```bash
   ls -l /opt/hetuengine-jdbc.jar
   ```

2. **Check JAR file permissions**
   ```bash
   # Make readable
   chmod 644 /opt/hetuengine-jdbc.jar

   # Verify permissions
   ls -l /opt/hetuengine-jdbc.jar
   # Should show -rw-r--r--
   ```

3. **Use absolute path**
   ```json
   {
     "connect_args": {
       "jar_path": "/opt/hetuengine-jdbc.jar"
     }
   }
   ```

4. **Verify JAR contains driver**
   ```bash
   jar -tf /opt/hetuengine-jdbc.jar | grep TrinoDriver
   # Should show: io/trino/jdbc/TrinoDriver.class
   ```

### Error: JAR File Not Found

**Symptoms:**
```
FileNotFoundError: JDBC driver JAR file not found at: /path/to/driver.jar
```

**Solutions:**

1. **Verify path is correct**
   ```bash
   # Check actual location
   find / -name "*hetuengine*.jar" 2>/dev/null
   find / -name "*trino*.jar" 2>/dev/null
   ```

2. **Download JDBC driver**
   - Contact Huawei support
   - Check HetuEngine installation directory
   - Typically in: `/opt/hetuengine/jdbc/` or similar

3. **Set environment variable**
   ```bash
   export HETUENGINE_JDBC_JAR=/path/to/hetuengine-jdbc.jar
   ```

## SSL/TLS Issues

### Error: SSL Handshake Failed

**Symptoms:**
```
SSL handshake failed
javax.net.ssl.SSLHandshakeException
```

**Solutions:**

1. **For self-signed certificates**
   ```json
   {
     "connect_args": {
       "jar_path": "/opt/hetuengine-jdbc.jar",
       "ssl": true,
       "ssl_verification": false
     }
   }
   ```

2. **Import certificate**
   ```bash
   # Import certificate to Java keystore
   keytool -import -alias hetuengine -file hetuengine.crt \
     -keystore $JAVA_HOME/jre/lib/security/cacerts \
     -storepass changeit
   ```

3. **Disable SSL (not recommended for production)**
   ```json
   {
     "connect_args": {
       "jar_path": "/opt/hetuengine-jdbc.jar",
       "ssl": false
     }
   }
   ```

### Error: Certificate Verification Failed

**Symptoms:**
```
Certificate verification failed
sun.security.validator.ValidatorException
```

**Solution:**

Disable SSL verification for self-signed certificates:
```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "ssl": true,
    "ssl_verification": false
  }
}
```

## Performance Issues

### Slow Query Execution

**Symptoms:**
- Queries take very long to execute
- Timeouts

**Solutions:**

1. **Increase timeout**
   ```json
   {
     "connect_args": {
       "jar_path": "/opt/hetuengine-jdbc.jar"
     },
     "engine_params": {
       "connect_timeout": 60,
       "read_timeout": 300
     }
   }
   ```

2. **Enable connection pooling**
   ```json
   {
     "engine_params": {
       "pool_size": 10,
       "max_overflow": 20,
       "pool_recycle": 3600
     }
   }
   ```

3. **Check HetuEngine cluster health**
   - Verify cluster has sufficient resources
   - Check for slow queries in HetuEngine UI
   - Review query execution plans

### Connection Pool Exhaustion

**Symptoms:**
```
TimeoutError: QueuePool limit exceeded
```

**Solution:**

Increase pool size:
```json
{
  "engine_params": {
    "pool_size": 20,
    "max_overflow": 40,
    "pool_timeout": 60
  }
}
```

## Query Issues

### Error: Schema/Table Not Found

**Symptoms:**
```
Schema 'xyz' does not exist
Table 'xyz' does not exist
```

**Solutions:**

1. **Verify catalog and schema**
   ```sql
   SHOW CATALOGS;
   SHOW SCHEMAS FROM hive;
   SHOW TABLES FROM hive.default;
   ```

2. **Check connection string**
   ```
   hetuengine://user:pass@host:29860/correct_catalog/correct_schema
   ```

3. **Verify user permissions**
   - Ensure user has access to catalog/schema
   - Check tenant permissions

### Error: Query Syntax Error

**Symptoms:**
```
SQL syntax error
mismatched input
```

**Solutions:**

1. **Use Trino/Presto syntax**
   - HetuEngine uses Trino SQL syntax
   - Reference: [Trino SQL Documentation](https://trino.io/docs/current/sql.html)

2. **Common syntax differences**
   ```sql
   -- Use DATE '2024-01-01' not '2024-01-01'
   SELECT * FROM table WHERE date_col = DATE '2024-01-01';

   -- Use LIMIT, not TOP
   SELECT * FROM table LIMIT 10;
   ```

## Docker-Specific Issues

### Error: Java Not Found in Container

**Symptoms:**
```
bash: java: command not found
```

**Solution:**

Update Dockerfile:
```dockerfile
FROM apache/superset:latest

USER root

# Install Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER superset
```

### Error: JAR File Not Accessible in Container

**Symptoms:**
```
FileNotFoundError: JAR file not found
```

**Solutions:**

1. **Copy JAR file in Dockerfile**
   ```dockerfile
   COPY hetuengine-jdbc.jar /opt/hetuengine-jdbc.jar
   RUN chmod 644 /opt/hetuengine-jdbc.jar
   ```

2. **Mount JAR file as volume**
   ```yaml
   volumes:
     - ./hetuengine-jdbc.jar:/opt/hetuengine-jdbc.jar:ro
   ```

### Error: Permission Denied in Container

**Symptoms:**
```
PermissionError: [Errno 13] Permission denied
```

**Solution:**

Fix file permissions in Dockerfile:
```dockerfile
COPY hetuengine-jdbc.jar /opt/hetuengine-jdbc.jar
RUN chmod 644 /opt/hetuengine-jdbc.jar && \
    chown superset:superset /opt/hetuengine-jdbc.jar
```

## Debugging Tips

### Enable Debug Logging

**In superset_config.py:**
```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# HetuEngine-specific logging
logging.getLogger('superset_hetuengine').setLevel(logging.DEBUG)
logging.getLogger('jaydebeapi').setLevel(logging.DEBUG)
logging.getLogger('jpype').setLevel(logging.DEBUG)
```

### Test Connection Outside Superset

```python
from superset_hetuengine.utils import test_jdbc_connection

success, error = test_jdbc_connection(
    jar_path="/opt/hetuengine-jdbc.jar",
    host="192.168.111.54",
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

### Check Java Installation

```python
from superset_hetuengine.utils import check_java_installation

is_installed, version = check_java_installation()
print(f"Java installed: {is_installed}")
print(f"Java version: {version}")
```

### Validate JDBC JAR

```python
from superset_hetuengine.utils import validate_jdbc_jar

is_valid, error = validate_jdbc_jar("/opt/hetuengine-jdbc.jar")
if is_valid:
    print("✓ JAR file is valid")
else:
    print(f"✗ JAR file error: {error}")
```

### Test with DBeaver First

Before configuring Superset:

1. Install DBeaver
2. Test connection with JDBC driver
3. Verify serviceDiscoveryMode and tenant work
4. Use exact same parameters in Superset

## Getting Help

If you're still experiencing issues:

1. **Check logs**
   ```bash
   # Superset logs
   tail -f superset.log

   # Docker logs
   docker logs -f superset
   ```

2. **Gather diagnostic information**
   - Superset version: `superset version`
   - Python version: `python --version`
   - Java version: `java -version`
   - Connector version: `pip show superset-hetuengine-connector`
   - Error messages and stack traces

3. **Report issue on GitHub**
   - [GitHub Issues](https://github.com/pesnik/superset-hetuengine-connector/issues)
   - Include diagnostic information
   - Redact sensitive information (passwords, IPs)

4. **Community support**
   - Apache Superset Slack
   - Stack Overflow (tag: `apache-superset`, `hetuengine`)

## Quick Reference

### Checklist for New Connections

- [ ] Java installed and JAVA_HOME set
- [ ] JDBC driver JAR file downloaded
- [ ] JAR file path is absolute
- [ ] JAR file has correct permissions (644)
- [ ] Host and port are correct
- [ ] serviceDiscoveryMode=hsbroker
- [ ] tenant parameter is set
- [ ] Username and password are correct
- [ ] Network connectivity verified
- [ ] SSL settings match server configuration
- [ ] Test connection succeeds

### Common Configuration

```json
{
  "connect_args": {
    "jar_path": "/opt/hetuengine-jdbc.jar",
    "service_discovery_mode": "hsbroker",
    "tenant": "default",
    "ssl": false
  }
}
```

### Working Example

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
    "tenant": "default"
  }
}
```
