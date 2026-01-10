# Contributing to Spatio

Thank you for your interest in contributing to Spatio! We welcome contributions from the community and are pleased to have you join us.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Review Process](#review-process)

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. We are committed to providing a welcoming and inspiring community for all.

### Our Standards

- Use welcoming and inclusive language
- Be respectful of differing viewpoints and experiences
- Gracefully accept constructive criticism
- Focus on what is best for the community
- Show empathy towards other community members

## Getting Started

### Prerequisites

- **Rust**: Install the latest stable version via [rustup](https://rustup.rs/)
- **Git**: For version control
- **A GitHub account**: For submitting pull requests

### Development Setup

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/spatio.git
   cd spatio
   ```
3. **Add the upstream repository**:
   ```bash
   git remote add upstream https://github.com/pkvartsianyi/spatio.git
   ```
4. **Install dependencies** and verify everything works:
   ```bash
   cargo build
   cargo test
   ```

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates.

**Good bug reports** include:
- A clear, descriptive title
- Steps to reproduce the issue
- Expected vs. actual behavior
- Code samples or minimal reproduction cases
- System information (OS, Rust version)
- Error messages or stack traces

### Suggesting Features

We welcome feature suggestions! Please:
- Check existing issues and discussions first
- Clearly describe the use case and benefit
- Consider the scope and complexity
- Be open to discussion and feedback

### Types of Contributions

We welcome various types of contributions:

- **Bug fixes**
- **New features** (spatial operations, performance improvements)
- **Documentation** improvements
- **Examples** and tutorials
- **Performance optimizations**
- **Test coverage** improvements

## Coding Standards

### Rust Style

- Follow the official [Rust Style Guide](https://doc.rust-lang.org/1.0.0/style/README.html)
- Use `cargo fmt` to format code
- Run `cargo clippy` and address all warnings
- Prefer explicit types when it improves readability
- Write descriptive variable and function names

### Code Organization

- Keep functions focused and single-purpose
- Use meaningful module organization
- Add comprehensive doc comments for public APIs
- Include usage examples in documentation

### Error Handling

- Use the existing `SpatioError` type for consistency
- Provide meaningful error messages
- Use `Result<T, SpatioError>` for fallible operations
- Avoid panics in library code

### Example Code Style

```rust
/// Calculate the distance between two geographic points.
///
/// Uses the Haversine formula to compute the great-circle distance
/// between two points on Earth's surface.
///
/// # Arguments
///
/// * `point1` - First geographic point
/// * `point2` - Second geographic point
///
/// # Returns
///
/// Distance in meters between the two points
///
/// # Examples
///
/// ```rust
/// use spatio::Point;
///
/// let nyc = Point::new(40.7128, -74.0060);
/// let london = Point::new(51.5074, -0.1278);
/// let distance = nyc.distance_to(&london);
/// assert!(distance > 5_000_000.0); // > 5000km
/// ```
pub fn distance_to(&self, other: &Point) -> f64 {
    // Implementation...
}
```

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run integration tests
cargo test --test integration_tests
```

### Writing Tests

- **Unit tests**: Test individual functions and methods
- **Integration tests**: Test public API functionality
- **Doc tests**: Ensure documentation examples work
- **Benchmarks**: For performance-critical code

### Test Guidelines

- Use descriptive test names: `test_spatial_query_within_radius`
- Test both success and error cases
- Use meaningful assertions with clear error messages
- Test edge cases and boundary conditions
- Keep tests focused and independent

### Example Test

```rust
#[test]
fn test_point_distance_calculation() {
    let nyc = Point::new(40.7128, -74.0060);
    let london = Point::new(51.5074, -0.1278);
    
    let distance = nyc.distance_to(&london);
    
    // Distance should be approximately 5585 km
    assert!((distance - 5_585_000.0).abs() < 100_000.0);
}
```

## Documentation

### API Documentation

- Write comprehensive doc comments for all public APIs
- Include usage examples that compile and run
- Document error conditions and edge cases
- Use proper Rust doc syntax (`///` for docs, `//!` for module docs)

### Examples

- Create focused examples for specific features
- Ensure examples are self-contained and runnable
- Add explanatory comments for complex operations
- Update examples when APIs change

### README Updates

- Keep the README current with new features
- Update usage examples when APIs change
- Maintain accurate feature lists and roadmap

## Submitting Changes

### Commit Messages

Use clear, descriptive commit messages:

```
feat: add bounding box intersection queries

- Implement BoundingBox struct with intersects() method
- Add find_within_bounds() method to DB
- Include comprehensive tests and documentation
- Update examples to demonstrate new functionality
```

**Format**: `<type>: <description>`

**Types**:
- `feat`: New features
- `fix`: Bug fixes
- `docs`: Documentation changes
- `test`: Test additions or fixes
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `chore`: Maintenance tasks

### Pull Request Process

1. **Create a branch** for your feature/fix:
   ```bash
   git checkout -b feature/spatial-intersections
   ```

2. **Make your changes** following the coding standards

3. **Test thoroughly**:
   ```bash
   cargo test
   cargo clippy
   cargo fmt --check
   ```

4. **Update documentation** as needed

5. **Commit your changes** with clear messages

6. **Push to your fork**:
   ```bash
   git push origin feature/spatial-intersections
   ```

7. **Create a Pull Request** on GitHub

### Pull Request Template

When submitting a PR, please include:

- **Description**: What does this PR do?
- **Motivation**: Why is this change needed?
- **Testing**: How was this tested?
- **Breaking changes**: Any API changes?
- **Checklist**: 
  - [ ] Tests pass (`cargo test`)
  - [ ] No clippy warnings (`cargo clippy`)
  - [ ] Code formatted (`cargo fmt`)
  - [ ] Documentation updated
  - [ ] Examples updated (if needed)

## Review Process

### What to Expect

- Initial response within 48 hours
- Code review and feedback
- Possible requests for changes
- Approval and merge once requirements are met

### Review Criteria

- **Functionality**: Does it work as intended?
- **Code quality**: Follows Rust best practices
- **Tests**: Adequate test coverage
- **Documentation**: Clear and comprehensive
- **Performance**: No significant regressions
- **Backwards compatibility**: Minimal breaking changes

### Addressing Feedback

- Respond to review comments promptly
- Make requested changes in new commits
- Ask questions if feedback is unclear
- Be open to suggestions and alternative approaches

## Development Tips

### Performance Considerations

- Profile before optimizing
- Consider algorithmic complexity
- Use appropriate data structures
- Benchmark performance-critical changes

### Debugging

- Use `cargo test -- --nocapture` for debug output
- Add temporary `dbg!()` macros for inspection
- Use `cargo run --example` to test specific scenarios

### Useful Commands

```bash
# Check everything is working
cargo check

# Run with optimizations
cargo build --release

# Generate documentation
cargo doc --open

# Run benchmarks
cargo bench

# Check test coverage (with tarpaulin)
cargo tarpaulin --out html
```

## Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Code Review**: Ask questions in PR comments

## Recognition

Contributors are recognized in:
- Git commit history
- Release notes for significant contributions
- GitHub contributor lists

Thank you for contributing to Spatio! Your efforts help make spatial data processing in Rust better for everyone.