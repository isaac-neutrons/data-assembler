"""
PyArrow schema definitions for lakehouse tables.

These schemas define the Parquet file structure for each model type,
ensuring compatibility with Apache Iceberg tables.
"""

import pyarrow as pa

# Schema for Reflectivity measurements
REFLECTIVITY_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("is_deleted", pa.bool_()),
        # Relationship fields (sample -> environment -> measurement)
        ("sample_id", pa.string()),
        ("environment_id", pa.string()),
        # Measurement fields
        ("proposal_number", pa.string()),
        ("facility", pa.string()),
        ("laboratory", pa.string()),
        ("probe", pa.string()),
        ("technique", pa.string()),
        ("technique_description", pa.string()),
        pa.field("is_simulated", pa.bool_(), metadata={b'description': b'Indicates if the measurement is simulated data'}),
        ("run_title", pa.string()),
        ("run_number", pa.string()),
        ("run_start", pa.timestamp("us", tz="UTC")),
        ("raw_file_path", pa.string()),
        ("instrument_name", pa.string()),
        # Reflectivity-specific fields
        (
            "reflectivity",
            pa.struct(
                [
                    ("measurement_geometry", pa.float64()),
                    ("reduction_time", pa.timestamp("us", tz="UTC")),
                    ("reduction_version", pa.string()),
                    ("reduction_parameters", pa.string()),  # JSON string
                    ("q", pa.list_(pa.float64())),
                    ("r", pa.list_(pa.float64())),
                    ("dr", pa.list_(pa.float64())),
                    ("dq", pa.list_(pa.float64())),
                ]
            ),
        ),
    ],
)

# Schema for Sample records
SAMPLE_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("is_deleted", pa.bool_()),
        # Sample fields
        ("description", pa.string()),
        ("main_composition", pa.string()),
        ("geometry", pa.string()),
        ("environment_ids", pa.list_(pa.string())),
        # Layers as JSON string (nested struct alternative)
        ("layers_json", pa.string()),
        (
            "layers",
            pa.list_(
                pa.struct(
                    [
                        ("layer_number", pa.int32()),
                        ("material", pa.string()),
                        ("thickness", pa.float64()),
                        ("roughness", pa.float64()),
                        ("sld", pa.float64()),
                    ]
                )
            ),
        ),
        ("substrate_json", pa.string()),
    ]
)

# Schema for Environment records
ENVIRONMENT_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        ("is_deleted", pa.bool_()),
        # Relationship field (sample -> environment)
        ("sample_id", pa.string()),
        # Environment fields
        ("description", pa.string()),
        ("ambient_medium", pa.string()),
        ("temperature", pa.float64()),
        ("pressure", pa.float64()),
        ("relative_humidity", pa.float64()),
        ("measurement_ids", pa.list_(pa.string())),
    ]
)


def get_schema_for_model(model_name: str) -> pa.Schema:
    """
    Get the PyArrow schema for a model type.

    Args:
        model_name: One of 'reflectivity', 'sample', 'environment'

    Returns:
        The corresponding PyArrow schema

    Raises:
        ValueError: If model_name is not recognized
    """
    schemas = {
        "reflectivity": REFLECTIVITY_SCHEMA,
        "sample": SAMPLE_SCHEMA,
        "environment": ENVIRONMENT_SCHEMA,
    }
    if model_name not in schemas:
        raise ValueError(f"Unknown model: {model_name}. Expected one of {list(schemas.keys())}")
    return schemas[model_name]
