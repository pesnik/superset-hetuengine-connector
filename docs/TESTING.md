# Testing HetuEngine Connector for Apache Superset

## Overview

This document provides comprehensive information about testing custom database connectors for Apache Superset, specifically focusing on the HetuEngine connector. It includes lessons learned, best practices, and recommended approaches based on research and implementation experience.

## Table of Contents

1. [Testing Approaches](#testing-approaches)
2. [Challenges Encountered](#challenges-encountered)
3. [Recommended Solutions](#recommended-solutions)
4. [Implementation Details](#implementation-details)
5. [Resources and References](#resources-and-references)

---

## Testing Approaches

### 1. Unit Tests (Complex - Not Recommended)

**Location**: `tests/unit_tests/`

**Approach**:
- Create isolated unit tests that mock Superset dependencies
- Test individual methods of your engine spec
- Require minimal Superset initialization

**Challenges**:
- Superset has complex initialization requirements at import time
- Many module-level singletons need to be initialized:
  - `encrypted_field_factory` - For encrypted database credentials
  - `event_logger` - For logging database events
  - `security_manager` - For access control
  - Flask app context with Babel (i18n)
- Importing `PrestoEngineSpec` triggers import of `superset.models.core` which requires:
  - Active Flask application context
  - Initialized SQLAlchemy models
  - All extension singletons properly configured
- Circular dependency issues and import order problems
- Marshmallow version compatibility issues with full app initialization

**Verdict**: ❌ **Not recommended** - Too complex for connector testing

### 2. Built-in `superset test-db` Command (Recommended)

**Approach**:
- Use Superset's built-in testing command
- Runs with a fully initialized Superset instance
- Tests connection and feature support automatically

**Usage**:
```bash
superset test-db hetuengine://user:password@host:port/catalog/schema
```

**What it tests**:
- ✅ Connection to the database
- ✅ SQLAlchemy dialect implements all necessary methods
- ✅ DB engine spec features supported
- ✅ End-to-end connectivity

**Advantages**:
- No complex test setup required
- Tests real functionality
- Validates entire integration
- Quick feedback on connector issues

**Verdict**: ✅ **Recommended** for quick validation and development

### 3. Integration Tests with Docker (Recommended for CI/CD)

**Location**: `tests/integration_tests/db_engine_specs/`

**Approach**:
- Run pytest with docker-compose
- Full Superset instance in containerized environment
- Test against real database connections

**Setup**:
```bash
# Use docker-compose for full integration tests
docker-compose up -d
pytest tests/integration_tests/db_engine_specs/test_hetuengine.py -v
```

**Pattern to follow**:
- See existing tests like MSSQL: `tests/integration_tests/db_engine_specs/test_mssql.py`
- Recent migrations to pytest: [PR #18251](https://github.com/apache/superset/pull/18251)

**Advantages**:
- Tests real integration
- CI/CD ready
- Proper environment isolation
- Matches production setup

**Verdict**: ✅ **Recommended** for comprehensive testing and CI/CD pipelines

---

## Challenges Encountered

### Challenge 1: Flask Application Context Required

**Problem**:
```python
RuntimeError: Working outside of application context.
```

**Root Cause**:
- Superset modules (`superset.models.helpers`) access `app.config` at import time
- Test files import HetuEngine modules → imports Superset modules → requires Flask context

**Solution**:
- Create Flask app and push context in `conftest.py` BEFORE any test imports
- Place initialization at module level (runs before pytest collects tests)

### Challenge 2: Extension Singletons Not Initialized

**Problem**:
```python
AttributeError: 'NoneType' object has no attribute 'log_this'
Exception: App not initialized yet. Please call init_app first
```

**Root Cause**:
- Superset uses module-level singletons in `superset.extensions`:
  - `event_logger` - Used by model decorators
  - `encrypted_field_factory` - Used for password columns
  - `security_manager` - Used for database event listeners
- These must be initialized BEFORE `superset.models.core` is imported

**Attempted Solutions**:
1. Initialize in `conftest.py` app creation - **Failed** (imports happen first)
2. Initialize at module level before app creation - **Partial success**
3. Mock all extensions before any imports - **Required for unit tests**

**Working Solution Pattern**:
```python
# In conftest.py - BEFORE any Superset imports
import superset.extensions
from unittest.mock import MagicMock

# Initialize event_logger
from superset.utils.log import AbstractEventLogger

class DummyEventLogger(AbstractEventLogger):
    def log(self, *args, **kwargs):
        pass
    def log_this(self, f):
        return f

superset.extensions.event_logger = DummyEventLogger()
superset.extensions.security_manager = MagicMock()

# Later, after app creation
from superset.extensions import encrypted_field_factory
encrypted_field_factory.init_app(app)
```

### Challenge 3: Import Order Dependencies

**Problem**:
- Test file imports → HetuEngine imports → Superset PrestoEngineSpec imports → Superset models import
- Each step has different initialization requirements
- Cannot control order after pytest starts collecting

**Solution**:
- All initialization must happen in `conftest.py` at module level
- `conftest.py` is loaded BEFORE test file imports
- Push app context immediately after creation

### Challenge 4: Marshmallow Compatibility

**Problem**:
```python
TypeError: Field.__init__() got an unexpected keyword argument 'minLength'
```

**Root Cause**:
- Superset's `SupersetAppInitializer.init_views()` imports schemas with marshmallow fields
- Marshmallow API version mismatch in environment
- Only triggered when doing full Superset app initialization

**Solution**:
- Skip full app initialization for unit tests
- Use `app.config.from_object("superset.config")` to load config
- Initialize only required extensions (Babel, encrypted_field_factory)
- Avoid calling `SupersetAppInitializer.init_app()`

### Challenge 5: SQLAlchemy Model Re-definition

**Problem**:
```python
sqlalchemy.exc.InvalidRequestError: Table 'dbs' is already defined for this MetaData instance
```

**Root Cause**:
- Models being imported multiple times in test collection
- SQLAlchemy metadata not properly cleared between test runs

**Solution**:
- Use session-scoped fixtures
- Avoid reimporting models in tests
- Consider using integration tests instead

---

## Recommended Solutions

### Solution 1: Use `superset test-db` for Development

**When to use**: During connector development and quick validation

**Steps**:
1. Install your connector package in Superset environment:
   ```bash
   pip install -e .
   ```

2. Test the connection:
   ```bash
   superset test-db hetuengine://user:password@host:port/catalog/schema
   ```

3. Verify output shows:
   - ✅ Connection successful
   - ✅ Dialect methods implemented
   - ✅ Features supported by engine spec

**Example Output**:
```
Testing database connection...
✓ Connection successful
✓ SQLAlchemy dialect: HetuEngineDialect
✓ DB engine spec: HetuEngineSpec
✓ Supports dynamic schema: False
✓ Supports file upload: False
...
```

### Solution 2: Simplified Unit Tests (If Required)

**When to use**: When you need automated tests without full Superset

**Strategy**: Test dialect and spec methods in isolation without importing PrestoEngineSpec

**Example**:
```python
# tests/unit_tests/test_hetuengine_isolated.py
import pytest
from superset_hetuengine.sqlalchemy_dialect import HetuEngineDialect

def test_dialect_name():
    """Test dialect name without Superset dependencies."""
    dialect = HetuEngineDialect()
    assert dialect.name == "hetuengine"
    assert dialect.driver == "hetuengine"

def test_jdbc_url_building():
    """Test JDBC URL construction."""
    dialect = HetuEngineDialect()
    url = dialect._build_jdbc_url(
        host="localhost",
        port=29860,
        catalog="hive",
        schema="default",
        connect_args={}
    )
    assert "serviceDiscoveryMode=hsbroker" in url
    assert "tenant=default" in url
```

**Advantages**:
- No Superset imports required
- Fast execution
- Easy to maintain
- Tests core functionality

**Limitations**:
- Doesn't test integration with Superset
- Doesn't test inheritance from PrestoEngineSpec
- Limited coverage

### Solution 3: Integration Tests (Best for CI/CD)

**When to use**: For comprehensive testing and continuous integration

**Setup**:
1. Create `tests/integration_tests/db_engine_specs/test_hetuengine.py`
2. Use docker-compose for test environment
3. Follow patterns from other database tests

**Example Structure**:
```python
# tests/integration_tests/db_engine_specs/test_hetuengine.py
import pytest
from superset.db_engine_specs import HetuEngineSpec
from tests.integration_tests.fixtures.database import get_example_database

@pytest.mark.integration
class TestHetuEngineEngineSpec:
    def test_engine_name(self):
        assert HetuEngineSpec.engine == "hetuengine"

    def test_convert_dttm(self):
        from datetime import datetime
        dttm = datetime(2024, 1, 15, 10, 30, 45)
        result = HetuEngineSpec.convert_dttm("TIMESTAMP", dttm)
        assert result == "TIMESTAMP '2024-01-15 10:30:45'"

    # More integration tests...
```

**Run with**:
```bash
pytest tests/integration_tests/db_engine_specs/test_hetuengine.py -v
```

---

## Implementation Details

### Current conftest.py Structure

The `tests/conftest.py` file has been set up with the following approach:

1. **Early Extension Initialization** (lines 23-42):
   - Initialize `event_logger` as DummyEventLogger
   - Mock `security_manager` with MagicMock
   - Done BEFORE any Superset model imports

2. **Flask App Creation** (lines 47-79):
   - Create Flask app
   - Load Superset config with `app.config.from_object("superset.config")`
   - Override test-specific settings
   - Initialize Flask-Babel
   - Initialize `encrypted_field_factory`

3. **Context Push** (lines 82-90):
   - Create app immediately at module import time
   - Push app context before test collection

4. **Pytest Fixtures** (lines 93-137):
   - `app` fixture: Provides Flask app instance
   - `client` fixture: Provides test client
   - `app_context` fixture: Auto-used for all tests

### Key Design Decisions

1. **Why module-level initialization?**
   - pytest collects test files before running fixtures
   - Test files import our modules → imports Superset → needs context
   - Only solution: Initialize before pytest collection starts

2. **Why not use SupersetAppInitializer?**
   - Full initialization causes marshmallow compatibility issues
   - Not needed for connector testing
   - Loading config file is sufficient for basic functionality

3. **Why mock extensions instead of initializing them?**
   - Real initialization requires database setup
   - Mocking is faster and simpler for unit tests
   - Tests don't actually use these features

### Known Limitations

1. **Unit tests still fragile**:
   - Dependent on Superset internal structure
   - May break with Superset upgrades
   - Requires ongoing maintenance

2. **Not testing actual PrestoEngineSpec inheritance**:
   - Mocked dependencies may not match real behavior
   - Integration tests or `test-db` needed for full validation

3. **SQL Alchemy model issues**:
   - Models may fail to load properly with mocked extensions
   - Some features may not work in mocked environment

---

## Resources and References

### Official Documentation

1. **Apache Superset DB Engine Specs README**
   - URL: https://github.com/apache/superset/blob/master/superset/db_engine_specs/README.md
   - Contains: Testing section, feature implementation guide

2. **Superset Development How-tos**
   - URL: https://superset.apache.org/docs/contributing/howtos/
   - Contains: Development workflows, testing best practices

3. **Superset Testing Guidelines**
   - URL: https://github.com/apache/superset/wiki/Testing-Guidelines-and-Best-Practices
   - Contains: Pytest setup, integration testing patterns

4. **Setting up Development Environment**
   - URL: https://superset.apache.org/docs/contributing/development/
   - Contains: Initial setup, database initialization

### Example Implementations

1. **MSSQL Test Migration to Pytest**
   - PR: https://github.com/apache/superset/pull/18251
   - Shows: How to migrate from integration to unit tests
   - Demonstrates: Parametrized testing patterns

2. **Database.get_extra() Comprehensive Tests**
   - Commit: https://www.mail-archive.com/commits@superset.apache.org/msg38351.html
   - Shows: Testing multiple engine specs
   - Demonstrates: Contract testing between models and specs

3. **Superset Unit Tests conftest.py**
   - URL: https://github.com/apache/superset/blob/master/tests/unit_tests/conftest.py
   - Shows: Official test configuration pattern
   - Downloaded for reference in this project

4. **Integration Test Configuration**
   - URL: https://github.com/apache/superset/blob/master/tests/integration_tests/superset_test_config.py
   - Shows: Full test environment configuration
   - Contains: Redis, cache, Celery setup

### Related Articles

1. **Improving Apache Superset Integration for a Database**
   - URL: https://preset.io/blog/improving-apache-superset-integration-database-sqlalchemy/
   - Contains: Checklist for database integration

2. **Connecting Apache Superset to Databases**
   - URL: https://superset.apache.org/docs/configuration/databases/
   - Contains: Database connection requirements

### Community Resources

1. **Testing Guidelines Wiki**
   - URL: https://github.com/apache/superset/wiki/Testing-Guidelines-and-Best-Practices
   - Community-maintained testing best practices

2. **Development Workflow Deep Dive**
   - URL: https://deepwiki.com/apache/superset/7.1-testing-and-quality-assurance
   - Detailed explanation of testing approaches

---

## Quick Reference Commands

### Testing the Connector

```bash
# Quick validation with built-in command
superset test-db hetuengine://user:password@host:port/catalog/schema

# Run unit tests (if implemented)
uv run pytest tests/unit_tests/ -v

# Run integration tests
uv run pytest tests/integration_tests/ -v

# Run with coverage
uv run pytest --cov=superset_hetuengine tests/ -v

# Run specific test file
uv run pytest tests/test_dialect.py -v

# Run specific test
uv run pytest tests/test_dialect.py::TestHetuEngineDialect::test_dialect_name -v
```

### Development Workflow

```bash
# 1. Install connector in development mode
pip install -e .

# 2. Quick validation
superset test-db hetuengine://...

# 3. Run automated tests
pytest tests/ -v

# 4. Check coverage
pytest --cov=superset_hetuengine --cov-report=html tests/
```

---

## Conclusions and Recommendations

### For Development
✅ **Use `superset test-db` command** - Fast, reliable, tests real functionality

### For CI/CD
✅ **Use integration tests with docker-compose** - Comprehensive, production-like

### For Quick Checks
✅ **Simplified isolated unit tests** - Test dialect methods without Superset imports

### Avoid
❌ **Complex unit tests with full Superset mocking** - Fragile, hard to maintain, breaks easily

---

## Future Work

1. **Implement integration test suite**
   - Create docker-compose test environment
   - Add comprehensive test cases
   - Set up CI/CD pipeline

2. **Simplify unit tests**
   - Refactor to test dialect in isolation
   - Remove PrestoEngineSpec dependency from tests
   - Mock only what's necessary

3. **Documentation**
   - Add connector usage examples
   - Document connection parameters
   - Create troubleshooting guide

4. **Superset Compatibility**
   - Test with different Superset versions
   - Document version requirements
   - Handle API changes gracefully

---

## Document History

- **Created**: 2025-12-11
- **Purpose**: Document testing challenges and solutions for HetuEngine connector
- **Audience**: Future developers, maintainers, and contributors
- **Status**: Living document - update as new approaches are discovered

---

## Contact and Support

For questions or issues related to testing this connector:
- GitHub Issues: https://github.com/[your-repo]/superset-hetuengine-connector/issues
- Superset Community: https://superset.apache.org/community/
- Superset Slack: https://apache-superset.slack.com/
