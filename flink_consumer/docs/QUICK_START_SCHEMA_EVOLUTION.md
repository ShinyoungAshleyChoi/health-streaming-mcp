# Quick Start: Schema Evolution

This guide provides a quick introduction to using schema evolution features in the Flink Iceberg Consumer.

## Prerequisites

- Schema Registry running at `http://localhost:8081`
- Iceberg catalog configured
- Flink consumer application set up

## Quick Examples

### 1. Check Avro Schema Compatibility

```python
from flink_consumer.converters import SchemaCompatibilityChecker

# Initialize checker
checker = SchemaCompatibilityChecker("http://localhost:8081")
checker.connect()

# Get latest schema
subject = "health-data-raw-value"
latest = checker.get_latest_schema(subject)
print(f"Latest schema version: {latest.version}")

# Validate schema evolution
is_valid, changes = checker.validate_schema_evolution(subject)

if is_valid:
    print("✓ Schema is compatible")
else:
    print("✗ Incompatible schema changes detected")
    for change in changes:
        if change.is_breaking:
            print(f"  - {change.description}")
```

### 2. Add Optional Column to Iceberg Table

```python
from flink_consumer.iceberg import IcebergSchemaEvolutionManager
from pyiceberg.types import StringType

# Initialize manager
manager = IcebergSchemaEvolutionManager(
    catalog_name="health_catalog",
    catalog_type="hadoop",
    warehouse="s3a://data-lake/warehouse"
)
manager.connect()

# Add optional column
success = manager.add_column(
    database="health_db",
    table_name="health_data_raw",
    column_name="device_model",
    column_type=StringType(),
    required=False,
    doc="Device model information"
)

if success:
    print("✓ Column added successfully")
```

### 3. Validate Schema Change Before Applying

```python
from flink_consumer.iceberg import SchemaOperation, SchemaOperationType

# Create operation
operation = SchemaOperation(
    operation_type=SchemaOperationType.ADD_COLUMN,
    column_name="new_field",
    column_type="string",
    required=False
)

# Validate
is_valid, error = manager.validate_schema_change(
    database="health_db",
    table_name="health_data_raw",
    operation=operation
)

if is_valid:
    # Safe to apply
    manager.add_column(...)
else:
    print(f"Validation failed: {error}")
```

### 4. View Schema History

```python
# Get schema evolution history
history = manager.get_schema_history("health_db", "health_data_raw")

for entry in history:
    print(f"{entry.timestamp}: {entry.description}")

# Or print formatted
manager.print_schema_history("health_db", "health_data_raw")
```

## Common Operations

### Add Optional Field (Safe)

```python
# Avro: Add to schema with default value
{
    "name": "newField",
    "type": ["null", "string"],
    "default": null
}

# Iceberg: Add optional column
manager.add_column(
    database="health_db",
    table_name="health_data_raw",
    column_name="new_field",
    column_type=StringType(),
    required=False
)
```

### Rename Column

```python
manager.rename_column(
    database="health_db",
    table_name="health_data_raw",
    old_column_name="old_name",
    new_column_name="new_name"
)
```

### Widen Type (int → long)

```python
from pyiceberg.types import LongType

manager.update_column_type(
    database="health_db",
    table_name="health_data_raw",
    column_name="count_field",
    new_type=LongType()
)
```

### Make Column Optional

```python
manager.make_column_optional(
    database="health_db",
    table_name="health_data_raw",
    column_name="optional_field"
)
```

## Testing

Run the test script to verify everything works:

```bash
cd flink_consumer
python examples/test_schema_evolution.py
```

## Integration with Flink Job

Add schema validation to your Flink application:

```python
from flink_consumer.converters import SchemaCompatibilityChecker
from flink_consumer.config.settings import settings

def main():
    # Validate schemas on startup
    checker = SchemaCompatibilityChecker(settings.schema_registry.url)
    checker.connect()
    
    subject = f"{settings.kafka.topic}-value"
    is_valid, changes = checker.validate_schema_evolution(subject)
    
    if not is_valid:
        raise RuntimeError("Incompatible schema detected!")
    
    # Continue with Flink job...
```

## Best Practices

1. **Always add optional fields** with default values
2. **Test in development** before applying to production
3. **Validate before applying** schema changes
4. **Monitor schema evolution** regularly
5. **Document changes** in version control

## Troubleshooting

### Schema Registry Not Accessible

```bash
# Check if Schema Registry is running
curl http://localhost:8081/subjects

# If not running, start it with docker-compose
docker-compose up -d schema-registry
```

### Incompatible Schema Changes

If you encounter incompatible changes:

1. Review the breaking changes
2. Add default values to new required fields
3. Make new fields optional
4. Consider using a new topic/table for major changes

### Iceberg Catalog Connection Issues

```bash
# Verify catalog configuration
echo $ICEBERG_CATALOG_URI
echo $ICEBERG_WAREHOUSE

# Check MinIO/S3 connectivity
aws s3 ls s3://data-lake/ --endpoint-url http://localhost:9000
```

## Next Steps

- Read the full [Schema Evolution Guide](SCHEMA_EVOLUTION.md)
- Review [Avro Schema Evolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
- Learn about [Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/)

## Support

For issues or questions:
1. Check the [Troubleshooting Guide](TROUBLESHOOTING.md)
2. Review the [Schema Evolution Guide](SCHEMA_EVOLUTION.md)
3. Consult the team documentation
