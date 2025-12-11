# Project Structure

This document describes the organization of the superset-hetuengine-connector project.

## Directory Tree

```
superset-hetuengine-connector/
├── .github/
│   └── workflows/
│       └── ci.yml                      # GitHub Actions CI/CD workflow
├── docs/
│   ├── configuration.md                # Configuration guide
│   ├── installation.md                 # Installation guide
│   └── troubleshooting.md              # Troubleshooting guide
├── examples/
│   ├── docker/
│   │   ├── Dockerfile                  # Docker image for Superset with HetuEngine
│   │   └── docker-compose.yml          # Docker Compose configuration
│   ├── config_examples.py              # Example Superset configuration
│   └── connection_examples.md          # Connection configuration examples
├── superset_hetuengine/
│   ├── __init__.py                     # Package initialization
│   ├── db_engine_spec.py              # Database engine specification
│   ├── sqlalchemy_dialect.py          # SQLAlchemy dialect
│   └── utils.py                        # Utility functions
├── tests/
│   ├── __init__.py                     # Test package initialization
│   ├── test_dialect.py                 # SQLAlchemy dialect tests
│   └── test_engine_spec.py             # Engine spec tests
├── .gitignore                          # Git ignore rules
├── CHANGELOG.md                        # Project changelog
├── ../CONTRIBUTING.md                     # Contribution guidelines
├── LICENSE                             # Apache 2.0 license
├── MANIFEST.in                         # Package manifest
├── PROJECT_STRUCTURE.md                # This file
├── ../QUICKSTART.md                       # Quick start guide
├── README.md                           # Main documentation
├── pytest.ini                          # Pytest configuration
├── requirements.txt                    # Python dependencies
├── setup.cfg                           # Setup configuration
└── setup.py                            # Package setup script
```

## Core Components

### superset_hetuengine/

The main package containing the connector implementation.

#### `__init__.py`
- Package initialization
- Exports main classes (HetuEngineSpec, HetuEngineDialect)
- Version information

#### `db_engine_spec.py`
- HetuEngine database engine specification
- Extends Superset's PrestoEngineSpec
- Handles HetuEngine-specific features:
  - Connection parameter extraction
  - Error message translation
  - Schema/table introspection
  - Time grain expressions
  - Data type conversions

**Key Classes:**
- `HetuEngineSpec`: Main engine specification class

**Key Methods:**
- `get_extra_params()`: Extract HetuEngine-specific parameters
- `build_sqlalchemy_uri()`: Build connection URI
- `extract_error_message()`: User-friendly error messages
- `validate_parameters()`: Connection parameter validation

#### `sqlalchemy_dialect.py`
- Custom SQLAlchemy dialect for HetuEngine
- JDBC bridge implementation using JayDeBeAPI
- Connection lifecycle management

**Key Classes:**
- `HetuEngineDialect`: SQLAlchemy dialect
- `HetuEngineCompiler`: SQL compiler
- `HetuEngineTypeCompiler`: Type compiler
- `HetuEngineIdentifierPreparer`: Identifier preparer

**Key Methods:**
- `create_connect_args()`: Build JDBC connection arguments
- `_build_jdbc_url()`: Construct JDBC URL
- `get_schema_names()`: List schemas
- `get_table_names()`: List tables
- `get_columns()`: Get column information
- `do_ping()`: Connection health check

#### `utils.py`
- Utility functions for connection testing and validation
- Java/JDBC environment checking
- Error message formatting

**Key Functions:**
- `test_jdbc_connection()`: Test JDBC connectivity
- `check_java_installation()`: Verify Java is installed
- `validate_jdbc_jar()`: Validate JDBC JAR file
- `build_jdbc_url()`: Construct JDBC URL
- `format_error_message()`: Format user-friendly errors

## Documentation

### README.md
Main project documentation covering:
- Overview and features
- Installation instructions
- Configuration examples
- Troubleshooting
- Docker deployment
- FAQ

### ../QUICKSTART.md
Quick start guide for getting up and running in 5 minutes.

### docs/installation.md
Detailed installation guide covering:
- Prerequisites (Java, JDBC driver, Superset)
- Installation methods (pip, source, Docker)
- Post-installation setup
- Verification steps

### docs/configuration.md
Configuration guide covering:
- Connection parameters
- Superset UI configuration
- Environment variables
- Docker configuration
- Security best practices
- Connection pooling

### docs/troubleshooting.md
Troubleshooting guide covering:
- Connection issues
- Java/JVM issues
- JDBC driver issues
- SSL/TLS issues
- Performance issues
- Docker-specific issues

## Examples

### examples/docker/
Docker deployment files:
- `Dockerfile`: Extends official Superset image with HetuEngine connector
- `docker-compose.yml`: Complete Docker Compose setup with Redis and PostgreSQL

### examples/config_examples.py
Example Superset configuration file with:
- HetuEngine-specific settings
- Cache configuration
- Security settings
- Feature flags

### examples/connection_examples.md
Connection configuration examples:
- Basic connections
- Multiple hosts
- SSL connections
- Custom tenants
- Programmatic configuration
- Migration from DBeaver

## Tests

### tests/test_engine_spec.py
Unit tests for HetuEngineSpec:
- Engine metadata tests
- Parameter extraction tests
- URI building tests
- Error message extraction tests
- Schema/table introspection tests

### tests/test_dialect.py
Unit tests for HetuEngineDialect:
- Connection argument creation
- JDBC URL building
- Schema/table operations
- Type resolution
- Connection health checks

## Configuration Files

### setup.py
Package setup script defining:
- Package metadata
- Dependencies
- Entry points (SQLAlchemy dialects, Superset engine specs)
- Classifiers

### setup.cfg
Extended setup configuration:
- Metadata
- Tool configurations (flake8, isort, mypy, coverage)
- Package options

### requirements.txt
Python dependencies:
- apache-superset
- jaydebeapi
- JPype1
- sqlalchemy

### pytest.ini
Pytest configuration:
- Test paths
- Test file patterns
- Markers (unit, integration, slow)
- Options

### MANIFEST.in
Package manifest specifying files to include in distribution.

## CI/CD

### .github/workflows/ci.yml
GitHub Actions workflow:
- Multi-platform testing (Ubuntu, macOS, Windows)
- Multi-version Python testing (3.8-3.11)
- Linting (flake8)
- Formatting checks (Black, isort)
- Type checking (mypy)
- Unit tests with coverage
- Package building and validation

## Contributing

### ../CONTRIBUTING.md
Contribution guidelines covering:
- Code of conduct
- How to contribute
- Development setup
- Coding standards
- Testing requirements
- Pull request process

### CHANGELOG.md
Project changelog following Keep a Changelog format.

## License

### LICENSE
Apache License 2.0 - full license text.

## File Count Summary

- Python source files: 4
- Test files: 2
- Documentation files: 8
- Configuration files: 6
- Docker files: 2
- Total files: ~25

## Lines of Code

- Core package: ~1,500 lines
- Tests: ~800 lines
- Documentation: ~3,000 lines
- Total: ~5,300 lines

## Key Features Implementation

1. **JDBC Bridge** → `sqlalchemy_dialect.py`
2. **HetuEngine Parameters** → `db_engine_spec.py::get_extra_params()`
3. **Error Handling** → `db_engine_spec.py::extract_error_message()`
4. **Connection Testing** → `utils.py::test_jdbc_connection()`
5. **Type Support** → `sqlalchemy_dialect.py::_resolve_type()`
6. **Multiple Hosts** → `sqlalchemy_dialect.py::_build_jdbc_url()`
7. **SSL Support** → `sqlalchemy_dialect.py::create_connect_args()`

## Development Workflow

1. **Setup**: Install with `pip install -e ".[dev]"`
2. **Code**: Make changes to source files
3. **Format**: Run `black` and `isort`
4. **Lint**: Run `flake8`
5. **Type Check**: Run `mypy`
6. **Test**: Run `pytest --cov`
7. **Document**: Update relevant docs
8. **Commit**: Follow commit message format
9. **PR**: Submit pull request

## Maintenance

### Regular Updates
- Update dependencies in `requirements.txt` and `setup.py`
- Update Superset compatibility
- Update Java/JDBC driver compatibility
- Review and update documentation

### Release Process
1. Update version in `setup.py` and `__init__.py`
2. Update `CHANGELOG.md`
3. Create git tag
4. Build package: `python -m build`
5. Upload to PyPI: `twine upload dist/*`
6. Create GitHub release

## External Dependencies

### Runtime Dependencies
- **apache-superset** (>=2.0.0): Core Superset framework
- **jaydebeapi** (>=1.2.3): JDBC bridge for Python
- **JPype1** (>=1.4.0): Java-Python bridge
- **sqlalchemy** (>=1.4.0,<2.0.0): SQL toolkit

### Development Dependencies
- **pytest**: Testing framework
- **pytest-cov**: Coverage reporting
- **black**: Code formatting
- **flake8**: Linting
- **mypy**: Type checking
- **isort**: Import sorting

### System Dependencies
- **Java** (11+): Required for JDBC
- **HetuEngine JDBC Driver**: Vendor-specific driver

## Support & Resources

- **GitHub Repository**: https://github.com/pesnik/superset-hetuengine-connector
- **Issue Tracker**: https://github.com/pesnik/superset-hetuengine-connector/issues
- **Documentation**: README.md and docs/
- **Examples**: examples/
- **Changelog**: CHANGELOG.md

---

For questions or contributions, see [../CONTRIBUTING.md](../CONTRIBUTING.md).
