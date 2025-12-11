# Contributing to superset-hetuengine-connector

Thank you for your interest in contributing to the HetuEngine connector for Apache Superset! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Release Process](#release-process)

## Code of Conduct

This project follows the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- **Clear title and description**
- **Steps to reproduce** the issue
- **Expected vs actual behavior**
- **Environment information**:
  - Superset version
  - Python version
  - Java version
  - HetuEngine version
  - Operating system
- **Error messages** and stack traces
- **Configuration** (redact sensitive information)

**Bug Report Template:**

```markdown
## Description
Brief description of the issue

## Steps to Reproduce
1. Step 1
2. Step 2
3. ...

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Environment
- Superset version: X.X.X
- Python version: X.X.X
- Java version: X.X.X
- HetuEngine version: X.X.X
- OS: [e.g., Ubuntu 22.04, macOS 13.0]

## Error Messages
```
Paste error messages and stack traces here
```

## Configuration
```json
{
  "connect_args": {
    "jar_path": "/path/to/jar",
    ...
  }
}
```
```

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, include:

- **Clear title and description**
- **Use case** - why is this enhancement useful?
- **Proposed solution** - how should it work?
- **Alternative solutions** - other approaches considered
- **Examples** - code samples or mockups if applicable

### Contributing Code

We welcome code contributions! Here's how to get started:

1. **Fork the repository**
2. **Create a feature branch**
3. **Make your changes**
4. **Write tests**
5. **Update documentation**
6. **Submit a pull request**

## Development Setup

### Prerequisites

- Python 3.8 or higher
- Java 11 or higher
- Git
- HetuEngine JDBC driver (for integration tests)

### Clone and Setup

```bash
# Fork and clone the repository
git clone https://github.com/YOUR_USERNAME/superset-hetuengine-connector.git
cd superset-hetuengine-connector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"

# Verify installation
python -c "import superset_hetuengine; print(superset_hetuengine.__version__)"
```

### Development Dependencies

The `[dev]` extra includes:

- **pytest** - Testing framework
- **pytest-cov** - Coverage reporting
- **black** - Code formatting
- **flake8** - Linting
- **mypy** - Type checking
- **isort** - Import sorting

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-number-description
```

### 2. Make Changes

Follow the coding standards (see below) when making changes.

### 3. Run Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_engine_spec.py

# Run with coverage
pytest --cov=superset_hetuengine --cov-report=html

# View coverage report
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

### 4. Format Code

```bash
# Format with Black
black superset_hetuengine tests

# Sort imports
isort superset_hetuengine tests

# Lint with flake8
flake8 superset_hetuengine tests

# Type check with mypy
mypy superset_hetuengine
```

### 5. Commit Changes

Write clear, descriptive commit messages:

```bash
git add .
git commit -m "feat: add support for custom connection timeout

- Add timeout parameter to connection configuration
- Update documentation with timeout examples
- Add tests for timeout functionality

Closes #123"
```

**Commit Message Format:**

```
<type>: <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `refactor`: Code refactoring
- `style`: Code style changes (formatting, etc.)
- `chore`: Maintenance tasks

### 6. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub.

## Coding Standards

### Python Style Guide

Follow [PEP 8](https://pep8.org/) style guide:

- Use 4 spaces for indentation (no tabs)
- Maximum line length: 88 characters (Black default)
- Use meaningful variable names
- Add docstrings to functions, classes, and modules

### Code Formatting

Use **Black** for consistent formatting:

```bash
black superset_hetuengine tests
```

### Import Sorting

Use **isort** for consistent import ordering:

```bash
isort superset_hetuengine tests
```

### Linting

Code must pass **flake8** linting:

```bash
flake8 superset_hetuengine tests
```

Configuration in `setup.cfg`:

```ini
[flake8]
max-line-length = 88
extend-ignore = E203, E501
exclude = .git,__pycache__,venv,build,dist
```

### Type Hints

Use type hints where possible:

```python
def connect(
    host: str,
    port: int,
    username: str,
    password: str,
) -> Connection:
    """Connect to HetuEngine database."""
    ...
```

Run **mypy** for type checking:

```bash
mypy superset_hetuengine
```

### Docstrings

Use Google-style docstrings:

```python
def example_function(param1: str, param2: int) -> bool:
    """
    Brief description of function.

    Longer description if needed.

    Args:
        param1: Description of param1
        param2: Description of param2

    Returns:
        Description of return value

    Raises:
        ValueError: When param2 is negative
    """
    if param2 < 0:
        raise ValueError("param2 must be non-negative")
    return True
```

## Testing

### Writing Tests

- Write tests for all new functionality
- Maintain or improve code coverage
- Use descriptive test names
- Follow the Arrange-Act-Assert pattern

**Example:**

```python
def test_connection_with_ssl():
    """Test that SSL parameters are correctly configured."""
    # Arrange
    mock_database = MagicMock()
    mock_database.encrypted_extra = {"ssl": True}

    # Act
    params = HetuEngineSpec.get_extra_params(mock_database)

    # Assert
    assert params["connect_args"]["ssl"] is True
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test
pytest tests/test_engine_spec.py::TestHetuEngineSpec::test_connection_with_ssl

# Run with coverage
pytest --cov=superset_hetuengine --cov-report=term-missing

# Generate HTML coverage report
pytest --cov=superset_hetuengine --cov-report=html
```

### Test Coverage

Aim for at least 80% code coverage. Check coverage with:

```bash
pytest --cov=superset_hetuengine --cov-report=term-missing
```

## Documentation

### Updating Documentation

When making changes, update relevant documentation:

- **README.md** - Main project documentation
- **docs/** - Detailed guides
- **Docstrings** - Code documentation
- **examples/** - Usage examples

### Documentation Style

- Use clear, concise language
- Include code examples
- Add tables for reference information
- Use proper markdown formatting
- Test all code examples

### Building Documentation

```bash
# Check for broken links in markdown
find . -name "*.md" -exec markdown-link-check {} \;
```

## Submitting Changes

### Pull Request Process

1. **Update documentation** for any new features
2. **Add tests** for new functionality
3. **Ensure all tests pass** locally
4. **Update CHANGELOG.md** with your changes
5. **Create pull request** with clear description

### Pull Request Template

```markdown
## Description
Brief description of changes

## Motivation and Context
Why is this change necessary? What problem does it solve?

## Type of Change
- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Documentation update

## Testing
- [ ] Unit tests pass locally
- [ ] Added new tests for functionality
- [ ] Integration tests pass (if applicable)
- [ ] Tested manually (describe test cases)

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-reviewed code
- [ ] Commented code, particularly in hard-to-understand areas
- [ ] Updated documentation
- [ ] No new warnings
- [ ] Added tests that prove fix/feature works
- [ ] New and existing tests pass locally
- [ ] Updated CHANGELOG.md

## Related Issues
Closes #<issue_number>
```

### Review Process

- Maintainers will review your pull request
- Address any feedback or requested changes
- Once approved, your PR will be merged
- Your contribution will be credited in the release notes

## Release Process

### Versioning

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR** version: Breaking changes
- **MINOR** version: New features (backward compatible)
- **PATCH** version: Bug fixes (backward compatible)

### Creating a Release

Maintainers will:

1. Update version in `setup.py` and `__init__.py`
2. Update CHANGELOG.md
3. Create git tag: `git tag -a v0.1.0 -m "Release v0.1.0"`
4. Push tag: `git push origin v0.1.0`
5. Create GitHub release
6. Publish to PyPI

## Getting Help

- **GitHub Issues** - For bug reports and feature requests
- **GitHub Discussions** - For questions and discussions
- **Slack** - Apache Superset Slack workspace
- **Email** - Contact maintainers (see README.md)

## Recognition

Contributors will be:

- Listed in CONTRIBUTORS.md
- Credited in release notes
- Acknowledged in the project

Thank you for contributing to superset-hetuengine-connector! 🎉
