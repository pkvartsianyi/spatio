//! Namespace support for Spatio
//!
//! This module provides namespace-aware key management for data isolation
//! and logical data organization within a single Spatio instance.

use crate::error::{Result, SpatioError};
use bytes::Bytes;
use std::fmt;

/// A namespace for organizing and isolating data
///
/// Namespaces provide logical separation of data within a single Spatio instance.
/// They are implemented using key prefixing with a configurable separator.
///
/// # Examples
///
/// ```rust
/// use spatio::Namespace;
///
/// let namespace_a = Namespace::new("namespace_a");
/// let namespace_b = Namespace::new("namespace_b");
///
/// // Keys are automatically prefixed
/// let key_a = namespace_a.key("user:123");
/// let key_b = namespace_b.key("user:123");
///
/// assert_ne!(key_a, key_b); // Different namespaces = different keys
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace {
    name: String,
    separator: String,
}

impl Namespace {
    /// Default namespace separator
    pub const DEFAULT_SEPARATOR: &'static str = "::";

    /// Create a new namespace with the default separator
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name (must not contain the separator)
    ///
    /// # Panics
    ///
    /// Panics if the name contains the default separator or is empty
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self::with_separator(name, Self::DEFAULT_SEPARATOR)
    }

    /// Create a new namespace with a custom separator
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name (must not contain the separator)
    /// * `separator` - The separator string to use between namespace and key
    ///
    /// # Panics
    ///
    /// Panics if the name contains the separator or if either is empty
    pub fn with_separator<S: Into<String>, T: Into<String>>(name: S, separator: T) -> Self {
        let name = name.into();
        let separator = separator.into();

        if name.is_empty() {
            panic!("Namespace name cannot be empty");
        }

        if separator.is_empty() {
            panic!("Namespace separator cannot be empty");
        }

        if name.contains(&separator) {
            panic!(
                "Namespace name '{}' cannot contain separator '{}'",
                name, separator
            );
        }

        Self { name, separator }
    }

    /// Get the namespace name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the separator used by this namespace
    pub fn separator(&self) -> &str {
        &self.separator
    }

    /// Create a namespaced key by prefixing the given key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to namespace
    ///
    /// # Returns
    ///
    /// A new key with the namespace prefix: `{namespace}{separator}{key}`
    pub fn key<K: AsRef<[u8]>>(&self, key: K) -> Bytes {
        let key_bytes = key.as_ref();
        let mut result =
            Vec::with_capacity(self.name.len() + self.separator.len() + key_bytes.len());

        result.extend_from_slice(self.name.as_bytes());
        result.extend_from_slice(self.separator.as_bytes());
        result.extend_from_slice(key_bytes);

        Bytes::from(result)
    }

    /// Create a namespaced key from a string
    pub fn key_str<S: AsRef<str>>(&self, key: S) -> Bytes {
        self.key(key.as_ref().as_bytes())
    }

    /// Get the prefix used by this namespace
    ///
    /// This is useful for scanning operations where you want all keys
    /// belonging to a specific namespace.
    pub fn prefix(&self) -> Bytes {
        let mut result = Vec::with_capacity(self.name.len() + self.separator.len());
        result.extend_from_slice(self.name.as_bytes());
        result.extend_from_slice(self.separator.as_bytes());
        Bytes::from(result)
    }

    /// Check if a key belongs to this namespace
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check
    ///
    /// # Returns
    ///
    /// `true` if the key starts with this namespace's prefix
    pub fn owns_key<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let prefix = self.prefix();
        key.as_ref().starts_with(&prefix)
    }

    /// Strip the namespace prefix from a key
    ///
    /// # Arguments
    ///
    /// * `namespaced_key` - A key that belongs to this namespace
    ///
    /// # Returns
    ///
    /// The original key without the namespace prefix, or None if the key
    /// doesn't belong to this namespace
    pub fn strip_prefix<K: AsRef<[u8]>>(&self, namespaced_key: K) -> Option<Bytes> {
        let key_bytes = namespaced_key.as_ref();
        let prefix = self.prefix();

        if key_bytes.starts_with(&prefix) {
            Some(Bytes::copy_from_slice(&key_bytes[prefix.len()..]))
        } else {
            None
        }
    }

    /// Validate a namespace name
    ///
    /// # Arguments
    ///
    /// * `name` - The namespace name to validate
    /// * `separator` - The separator that will be used
    ///
    /// # Returns
    ///
    /// `Ok(())` if valid, `Err(SpatioError)` if invalid
    pub fn validate_name(name: &str, separator: &str) -> Result<()> {
        if name.is_empty() {
            return Err(SpatioError::Other("Namespace name cannot be empty".into()));
        }

        if name.contains(separator) {
            return Err(SpatioError::Other(format!(
                "Namespace name '{}' cannot contain separator '{}'",
                name, separator
            )));
        }

        // Additional validation rules
        if name.contains('\0') {
            return Err(SpatioError::Other(
                "Namespace name cannot contain null bytes".into(),
            ));
        }

        if name.len() > 255 {
            return Err(SpatioError::Other(
                "Namespace name cannot exceed 255 characters".into(),
            ));
        }

        Ok(())
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Self::new("default")
    }
}

/// Namespace-aware key manager
///
/// This provides utilities for working with multiple namespaces and
/// extracting namespace information from keys.
#[derive(Debug, Clone)]
pub struct NamespaceManager {
    separator: String,
}

impl NamespaceManager {
    /// Create a new namespace manager with the default separator
    pub fn new() -> Self {
        Self::with_separator(Namespace::DEFAULT_SEPARATOR)
    }

    /// Create a new namespace manager with a custom separator
    pub fn with_separator<S: Into<String>>(separator: S) -> Self {
        let separator = separator.into();
        if separator.is_empty() {
            panic!("Separator cannot be empty");
        }
        Self { separator }
    }

    /// Get the separator used by this manager
    pub fn separator(&self) -> &str {
        &self.separator
    }

    /// Create a namespace
    pub fn namespace<S: Into<String>>(&self, name: S) -> Namespace {
        Namespace::with_separator(name, &self.separator)
    }

    /// Extract namespace and key from a namespaced key
    ///
    /// # Arguments
    ///
    /// * `namespaced_key` - The full namespaced key
    ///
    /// # Returns
    ///
    /// A tuple of (namespace_name, original_key) if the key is namespaced,
    /// or None if it's not in namespace format
    pub fn parse_key<K: AsRef<[u8]>>(&self, namespaced_key: K) -> Option<(String, Bytes)> {
        let key_bytes = namespaced_key.as_ref();
        let separator_bytes = self.separator.as_bytes();

        // Find the first occurrence of the separator
        let key_str = std::str::from_utf8(key_bytes).ok()?;
        let separator_pos = key_str.find(&self.separator)?;

        let namespace_name = key_str[..separator_pos].to_string();
        let original_key =
            Bytes::copy_from_slice(&key_bytes[separator_pos + separator_bytes.len()..]);

        Some((namespace_name, original_key))
    }

    /// Get all unique namespace names from a collection of keys
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator over keys to analyze
    ///
    /// # Returns
    ///
    /// A vector of unique namespace names found in the keys
    pub fn extract_namespaces<'a, I, K>(&self, keys: I) -> Vec<String>
    where
        I: Iterator<Item = &'a K>,
        K: AsRef<[u8]> + 'a,
    {
        let mut namespaces = std::collections::HashSet::new();

        for key in keys {
            if let Some((namespace, _)) = self.parse_key(key) {
                namespaces.insert(namespace);
            }
        }

        let mut result: Vec<String> = namespaces.into_iter().collect();
        result.sort();
        result
    }

    /// Check if a key belongs to a specific namespace
    pub fn key_belongs_to_namespace<K: AsRef<[u8]>>(&self, key: K, namespace_name: &str) -> bool {
        if let Some((ns, _)) = self.parse_key(key) {
            ns == namespace_name
        } else {
            false
        }
    }
}

impl Default for NamespaceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_creation() {
        let ns = Namespace::new("test_namespace");
        assert_eq!(ns.name(), "test_namespace");
        assert_eq!(ns.separator(), "::");
    }

    #[test]
    fn test_namespace_with_custom_separator() {
        let ns = Namespace::with_separator("test", ":");
        assert_eq!(ns.name(), "test");
        assert_eq!(ns.separator(), ":");
    }

    #[test]
    #[should_panic(expected = "Namespace name cannot be empty")]
    fn test_empty_namespace_name() {
        Namespace::new("");
    }

    #[test]
    #[should_panic(expected = "cannot contain separator")]
    fn test_namespace_name_with_separator() {
        Namespace::new("test::invalid");
    }

    #[test]
    fn test_key_creation() {
        let ns = Namespace::new("tenant_a");
        let key = ns.key("user:123");
        assert_eq!(key, Bytes::from("tenant_a::user:123"));
    }

    #[test]
    fn test_key_str_creation() {
        let ns = Namespace::new("tenant_a");
        let key = ns.key_str("user:123");
        assert_eq!(key, Bytes::from("tenant_a::user:123"));
    }

    #[test]
    fn test_prefix() {
        let ns = Namespace::new("tenant_a");
        let prefix = ns.prefix();
        assert_eq!(prefix, Bytes::from("tenant_a::"));
    }

    #[test]
    fn test_owns_key() {
        let ns = Namespace::new("tenant_a");
        let key = ns.key("user:123");

        assert!(ns.owns_key(&key));
        assert!(!ns.owns_key(b"other::user:123"));
        assert!(!ns.owns_key(b"user:123"));
    }

    #[test]
    fn test_strip_prefix() {
        let ns = Namespace::new("tenant_a");
        let original_key = b"user:123";
        let namespaced_key = ns.key(original_key);

        let stripped = ns.strip_prefix(&namespaced_key).unwrap();
        assert_eq!(stripped, Bytes::from(&original_key[..]));

        // Test with non-owned key
        assert!(ns.strip_prefix(b"other::user:123").is_none());
    }

    #[test]
    fn test_namespace_validation() {
        assert!(Namespace::validate_name("valid_name", "::").is_ok());
        assert!(Namespace::validate_name("", "::").is_err());
        assert!(Namespace::validate_name("invalid::name", "::").is_err());
        assert!(Namespace::validate_name("null\0byte", "::").is_err());

        let long_name = "a".repeat(256);
        assert!(Namespace::validate_name(&long_name, "::").is_err());
    }

    #[test]
    fn test_namespace_manager() {
        let manager = NamespaceManager::new();
        let ns = manager.namespace("namespace_a");
        let key = ns.key("user:123");

        let (parsed_ns, parsed_key) = manager.parse_key(&key).unwrap();
        assert_eq!(parsed_ns, "namespace_a");
        assert_eq!(parsed_key, Bytes::from("user:123"));
    }

    #[test]
    fn test_namespace_manager_parse_non_namespaced_key() {
        let manager = NamespaceManager::new();
        assert!(manager.parse_key(b"simple_key").is_none());
    }

    #[test]
    fn test_extract_namespaces() {
        let manager = NamespaceManager::new();
        let keys = [
            Bytes::from("namespace_a::user:1"),
            Bytes::from("namespace_b::user:2"),
            Bytes::from("namespace_a::order:1"),
            Bytes::from("simple_key"),
        ];

        let namespaces = manager.extract_namespaces(keys.iter());
        assert_eq!(namespaces, vec!["namespace_a", "namespace_b"]);
    }

    #[test]
    fn test_key_belongs_to_namespace() {
        let manager = NamespaceManager::new();
        let key = Bytes::from("namespace_a::user:123");

        assert!(manager.key_belongs_to_namespace(&key, "namespace_a"));
        assert!(!manager.key_belongs_to_namespace(&key, "namespace_b"));
        assert!(!manager.key_belongs_to_namespace(b"simple_key", "namespace_a"));
    }

    #[test]
    fn test_namespace_display() {
        let ns = Namespace::new("test_namespace");
        assert_eq!(format!("{}", ns), "test_namespace");
    }

    #[test]
    fn test_namespace_default() {
        let ns = Namespace::default();
        assert_eq!(ns.name(), "default");
    }

    #[test]
    fn test_different_separators() {
        let ns1 = Namespace::with_separator("test", "::");
        let ns2 = Namespace::with_separator("test", ":");
        let ns3 = Namespace::with_separator("test", "/");

        assert_eq!(ns1.key("key"), Bytes::from("test::key"));
        assert_eq!(ns2.key("key"), Bytes::from("test:key"));
        assert_eq!(ns3.key("key"), Bytes::from("test/key"));
    }

    #[test]
    fn test_namespace_equality() {
        let ns1 = Namespace::new("test");
        let ns2 = Namespace::new("test");
        let ns3 = Namespace::new("other");

        assert_eq!(ns1, ns2);
        assert_ne!(ns1, ns3);
    }

    #[test]
    fn test_binary_keys() {
        let ns = Namespace::new("binary");
        let binary_key = vec![0u8, 1, 2, 255, 254];
        let namespaced = ns.key(&binary_key);

        assert!(ns.owns_key(&namespaced));
        let stripped = ns.strip_prefix(&namespaced).unwrap();
        assert_eq!(stripped.as_ref(), binary_key.as_slice());
    }
}
