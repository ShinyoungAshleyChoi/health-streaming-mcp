# Schema Evolution Guide

This guide explains how to handle schema evolution in the Flink Iceberg Consumer application, covering both Avro schema evolution (Schema Registry) and Iceberg table schema evolution.

## Overview

Schema evolution allows you to modify data schemas over time while maintaining compatibility with existing data and applications. The Flink Iceberg Consumer supports:

1. **Avro Schema Evolution**: Managing schema changes in Kafka messages via Schema Registry
2. **Iceberg Schema Evolution**: Evolving table schemas in the data lake

## Avro Schema Evolution (Schema Registry)

### Schema Compatibility Checker

The `SchemaCompatibilityChecker` class provides utilities for managing Avro schema evolution:

```python
from converters.schema_evolution import SchemaCompatibilityChecker, CompatibilityType

# Initialize checker
checker = SchemaCompatibilityChecker(schema_registry_url="http://localhost:8081")
checker.connect()

# Get latest schema
subject = "health-data-raw-value"
latest_schema = checker.get_latest_schema(subject)

# Validate schema evolution
is_valid, changes = checker.validate_schema_evolution(subject)

if not is_valid:
    # Handle incompatible changes
    checker.handle_incompatible_schema(subject, changes)
```

### Compatibility Types

Schema Registry supports different compatibility modes:

- **BACKWARD**: New schema can read old data (default)
  - Safe: Add optional fields, remove fields
  - Breaking: Add required fields, change field types

- **FORWARD**: Old schema can read new data
  - Safe: Remove fields, add optional fields
  - Breaking: Add required fields

- **FULL**: Both backward and forward compatible
  - Safe: Add optional fields only
  - Breaking: Any other change

- **NONE**: No compatibility checking

### Checking Schema Compatibility

```python
# Check if a new schema is compatible
new_schema_str = '''
{
    "type": "record",
    "name": "HealthData",
    "fields": [
        {"name": "deviceId", "type": "string"},
        {"name": "userId", "type": "string"},
        {"name": "newField", "type": ["null", "string"], "default": null}
    ]
}
'''

is_compatible, errors = checker.check_compatibility(
    subject="health-data-raw-value",
    new_schema_str=new_schema_str,
    compatibility_type=CompatibilityType.BACKWARD
)

if not is_compatible:
    for error in errors:
        print(f"Compatibility error: {error}")
```

### Comparing Schema Versions

```python
# Get two schema versions
old_schema = checker.get_schema_by_version(subject, version=1)
new_schema = checker.get_schema_by_version(subject, version=2)

# Compare schemas
changes = checker.compare_schemas(old_schema, new_schema)

for change in changes:
    if change.is_breaking:
        print(f"Breaking change: {change.description}")
    else:
        print(f"Compatible change: {change.description}")
```

### Schema Change Types

The system detects the following types of schema changes:

1. **FIELD_ADDED**: New field added to schema
   - Breaking if required without default value
   - Compatible if optional or has default value

2. **FIELD_REMOVED**: Field removed from schema
   - Always breaking for backward compatibility

3. **FIELD_TYPE_CHANGED**: Field type modified
   - Compatible if type widening (int → long, float → double)
   - Breaking for incompatible type changes

4. **FIELD_RENAMED**: Field name changed
   - Breaking without proper aliasing

5. **DEFAULT_VALUE_CHANGED**: Default value modified
   - Usually compatible

## Iceberg Schema Evolution

### Schema Evolution Manager

The `IcebergSchemaEvolutionManager` class provides utilities for evolving Iceberg table schemas:

```python
from iceberg.schema_evolution import IcebergSchemaEvolutionManager
from pyiceberg.types import StringType, DoubleType

# Initialize manager
manager = IcebergSchemaEvolutionManager(
    catalog_name="health_catalog",
    catalog_type="hadoop",
    warehouse="s3a://data-lake/warehouse"
)
manager.connect()
```

### Adding Columns

```python
# Add optional column (safe)
success = manager.add_column(
    database="health_db",
    table_name="health_data_raw",
    column_name="device_model",
    column_type=StringType(),
    required=False,
    doc="Device model information"
)

if success:
    print("Column added successfully")
```

### Removing Columns

```python
# Drop column (marks as deleted, doesn't physically remove)
success = manager.drop_column(
    database="health_db",
    table_name="health_data_raw",
    column_name="old_field"
)
```

### Renaming Columns

```python
# Rename column
success = manager.rename_column(
    database="health_db",
    table_name="health_data_raw",
    old_column_name="user_id",
    new_column_name="patient_id"
)
```

### Updating Column Types

```python
# Update column type (only compatible promotions allowed)
from pyiceberg.types import LongType

success = manager.update_column_type(
    database="health_db",
    table_name="health_data_raw",
    column_name="count_field",
    new_type=LongType()  # Promoting from IntegerType
)
```

### Making Columns Optional

```python
# Make required column optional
success = manager.make_column_optional(
    database="health_db",
    table_name="health_data_raw",
    column_name="optional_field"
)
```

### Validating Schema Changes

```python
from iceberg.schema_evolution import SchemaOperation, SchemaOperationType

# Create operation
operation = SchemaOperation(
    operation_type=SchemaOperationType.ADD_COLUMN,
    column_name="new_field",
    column_type="string",
    required=False
)

# Validate before applying
is_valid, error = manager.validate_schema_change(
    database="health_db",
    table_name="health_data_raw",
    operation=operation
)

if is_valid:
    # Apply the change
    manager.add_column(...)
else:
    print(f"Validation failed: {error}")
```

### Viewing Schema History

```python
# Get schema evolution history
history = manager.get_schema_history(
    database="health_db",
    table_name="health_data_raw"
)

for entry in history:
    print(f"Timestamp: {entry.timestamp}")
    print(f"Description: {entry.description}")
    for op in entry.operations:
        print(f"  - {op.operation_type.value}: {op.column_name}")

# Or print formatted history
manager.print_schema_history("health_db", "health_data_raw")
```

## Safe Schema Evolution Patterns

### ✅ Safe Changes (Backward Compatible)

1. **Add optional field with default value**
   ```python
   # Avro
   {"name": "newField", "type": ["null", "string"], "default": null}
   
   # Iceberg
   manager.add_column(..., required=False)
   ```

2. **Make required field optional**
   ```python
   manager.make_column_optional(...)
   ```

3. **Widen numeric types**
   ```python
   # int → long
   # float → double
   manager.update_column_type(..., new_type=LongType())
   ```

4. **Add documentation**
   ```python
   manager.add_column(..., doc="Field description")
   ```

### ❌ Breaking Changes (Not Backward Compatible)

1. **Add required field without default**
   ```python
   # DON'T DO THIS
   {"name": "requiredField", "type": "string"}  # No default!
   ```

2. **Remove existing field**
   ```python
   # Use with caution - breaks old readers
   manager.drop_column(...)
   ```

3. **Change field type incompatibly**
   ```python
   # DON'T DO THIS
   # string → int
   # long → int (narrowing)
   ```

4. **Rename field without alias**
   ```python
   # Use carefully - may break consumers
   manager.rename_column(...)
   ```

## Best Practices

### 1. Always Add Optional Fields

When adding new fields, make them optional with default values:

```python
# Good
manager.add_column(
    column_name="new_field",
    column_type=StringType(),
    required=False,  # Optional
    doc="Description"
)

# Bad - breaks backward compatibility
manager.add_column(
    column_name="new_field",
    column_type=StringType(),
    required=True,  # Required without default!
)
```

### 2. Test Schema Changes First

Always test schema changes in a development environment:

```python
# Validate before applying
is_valid, error = manager.validate_schema_change(database, table, operation)

if not is_valid:
    logger.error(f"Schema change validation failed: {error}")
    return

# Apply the change
success = manager.add_column(...)
```

### 3. Use Schema Registry Compatibility Mode

Configure Schema Registry with appropriate compatibility mode:

```bash
# Set compatibility mode for subject
curl -X PUT http://localhost:8081/config/health-data-raw-value \
  -H "Content-Type: application/json" \
  -d '{"compatibility": "BACKWARD"}'
```

### 4. Document Schema Changes

Log all schema changes with clear descriptions:

```python
# Schema changes are automatically logged
manager.add_column(
    ...,
    doc="Added in v2.0 to support device model tracking"
)

# View history
manager.print_schema_history(database, table)
```

### 5. Monitor Schema Evolution

Check schema compatibility regularly:

```python
# In your Flink application
checker = SchemaCompatibilityChecker(schema_registry_url)

# Validate on startup
is_valid, changes = checker.validate_schema_evolution(subject)

if not is_valid:
    logger.error("Incompatible schema detected!")
    # Handle error (alert, stop processing, etc.)
```

### 6. Handle Schema Evolution Errors

Implement proper error handling:

```python
try:
    is_valid, changes = checker.validate_schema_evolution(subject)
    
    if not is_valid:
        checker.handle_incompatible_schema(subject, changes)
        
except ValueError as e:
    logger.error(f"Schema evolution error: {e}")
    # Alert operations team
    # Stop processing or use fallback
```

## Integration with Flink Application

### Startup Schema Validation

Add schema validation to your Flink application startup:

```python
from converters.schema_evolution import SchemaCompatibilityChecker
from config.settings import settings

def validate_schemas_on_startup():
    """Validate schemas before starting Flink job"""
    checker = SchemaCompatibilityChecker(settings.schema_registry.url)
    checker.connect()
    
    subject = f"{settings.kafka.topic}-value"
    is_valid, changes = checker.validate_schema_evolution(
        subject,
        allow_breaking_changes=False
    )
    
    if not is_valid:
        logger.error("Schema validation failed on startup!")
        for change in changes:
            if change.is_breaking:
                logger.error(f"  - {change.description}")
        raise RuntimeError("Incompatible schema detected")
    
    logger.info("Schema validation passed")
    return True

# In main.py
if __name__ == "__main__":
    validate_schemas_on_startup()
    # Continue with Flink job setup...
```

### Runtime Schema Monitoring

Monitor schema changes during runtime:

```python
from pyflink.datastream import MapFunction

class SchemaMonitoringFunction(MapFunction):
    """Monitor schema changes during processing"""
    
    def open(self, runtime_context):
        self.checker = SchemaCompatibilityChecker(schema_registry_url)
        self.checker.connect()
        self.last_check = time.time()
        self.check_interval = 300  # Check every 5 minutes
    
    def map(self, value):
        # Periodically check for schema changes
        current_time = time.time()
        if current_time - self.last_check > self.check_interval:
            self._check_schema_evolution()
            self.last_check = current_time
        
        return value
    
    def _check_schema_evolution(self):
        is_valid, changes = self.checker.validate_schema_evolution(subject)
        
        if not is_valid:
            logger.warning("Schema evolution detected!")
            # Log changes but continue processing
            for change in changes:
                logger.warning(f"  - {change.description}")
```

## Testing Schema Evolution

Run the test script to verify schema evolution functionality:

```bash
cd flink_consumer
python examples/test_schema_evolution.py
```

This will test:
- Avro schema compatibility checking
- Iceberg schema evolution operations
- Schema change validation
- Schema history tracking

## Troubleshooting

### Schema Registry Connection Issues

```python
# Check Schema Registry connectivity
try:
    checker = SchemaCompatibilityChecker(schema_registry_url)
    checker.connect()
    logger.info("Connected to Schema Registry")
except Exception as e:
    logger.error(f"Failed to connect: {e}")
    # Check if Schema Registry is running
    # Verify URL is correct
```

### Incompatible Schema Changes

```python
# If you encounter incompatible changes:
# 1. Review the changes
is_valid, changes = checker.validate_schema_evolution(subject)

for change in changes:
    if change.is_breaking:
        print(f"Breaking: {change.description}")

# 2. Options:
#    a) Revert the schema change
#    b) Add default values to new required fields
#    c) Make new fields optional
#    d) Use a new topic/table for incompatible changes
```

### Iceberg Schema Update Failures

```python
# If schema update fails:
try:
    success = manager.add_column(...)
except Exception as e:
    logger.error(f"Schema update failed: {e}")
    # Common causes:
    # - Column already exists
    # - Incompatible type change
    # - Catalog connection issues
    # - Insufficient permissions
```

## References

- [Apache Avro Schema Evolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Apache Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)

## Summary

Schema evolution is a critical aspect of maintaining data pipelines. Key takeaways:

1. **Always test schema changes** in development before production
2. **Add optional fields** with default values for backward compatibility
3. **Monitor schema evolution** regularly using the provided tools
4. **Document all changes** for future reference
5. **Handle errors gracefully** to prevent pipeline failures

For questions or issues, refer to the troubleshooting section or consult the team.
