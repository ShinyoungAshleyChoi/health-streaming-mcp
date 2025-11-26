#!/usr/bin/env python3
"""
Avro schema validation script
"""
import json
import sys
from pathlib import Path

try:
    import fastavro
except ImportError:
    print("Error: fastavro not installed. Run: uv pip install fastavro")
    sys.exit(1)


def validate_schema(schema_path: Path) -> bool:
    """Validate an Avro schema file"""
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        # Parse schema to validate it
        parsed_schema = fastavro.parse_schema(schema)
        
        print(f"✓ Schema '{schema_path.name}' is valid")
        print(f"  Name: {parsed_schema.get('name')}")
        print(f"  Namespace: {parsed_schema.get('namespace')}")
        print(f"  Fields: {len(parsed_schema.get('fields', []))}")
        
        return True
        
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in '{schema_path.name}': {e}")
        return False
    except Exception as e:
        print(f"✗ Schema validation failed for '{schema_path.name}': {e}")
        return False


def main():
    """Validate all Avro schemas in the schemas directory"""
    schemas_dir = Path(__file__).parent
    schema_files = list(schemas_dir.glob("*.avsc"))
    
    if not schema_files:
        print("No schema files found (*.avsc)")
        sys.exit(1)
    
    print(f"Validating {len(schema_files)} schema file(s)...\n")
    
    all_valid = True
    for schema_file in schema_files:
        if not validate_schema(schema_file):
            all_valid = False
        print()
    
    if all_valid:
        print("All schemas are valid!")
        sys.exit(0)
    else:
        print("Some schemas are invalid!")
        sys.exit(1)


if __name__ == "__main__":
    main()
