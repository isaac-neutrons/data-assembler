"""
PyArrow schema definitions for lakehouse tables.

These schemas define the Parquet file structure for each model type,
ensuring compatibility with Apache Iceberg tables.
"""

import pyarrow as pa

#material properties
material = pa.struct(
    [
        ("composition", pa.string()),
        ("mass", pa.float64()),
        ("density", pa.float64()),
    ]
)

#layer properties
layer = pa.struct(
    [
        ("material", material),
        ("thickness", pa.float64()),
        ("roughness", pa.float64()),
        ("sld", pa.float64()),
    ]
)

# Schema for Reflectivity measurements
REFLECTIVITY_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        # ("is_deleted", pa.bool_()),
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
        ("instrument", pa.string()),
        # Reflectivity-specific fields
        ("measurement_geometry", pa.string()),
        ("reduction_time", pa.timestamp("us", tz="UTC")),
        ("reduction_version", pa.string()),
        ("q_1_angstrom", pa.list_(pa.float64())),
        ("r", pa.list_(pa.float64())),
        ("d_r", pa.list_(pa.float64())),
        ("d_q", pa.list_(pa.float64())),
    ],
)

# Schema for Sample records
SAMPLE_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        # Sample fields
        ("description", pa.string()),
        ("main_composition", pa.string()),
        ("geometry", pa.string()),
        ("environment_ids", pa.list_(pa.string())),
        # Layers as JSON string (nested struct alternative)
        #("layers_json", pa.string()),
        (
            "layers",
            pa.list_(
                #layers are already sorted
                layer
            ),
        ),
        # subrater is one layer
        ("substrate",layer),
        #for publications
        ("publication_ids", pa.list_(pa.string())),

    ]
)

# Schema for Environment records
ENVIRONMENT_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        #("is_deleted", pa.bool_()),
        # Environment fields
        ("description", pa.string()),
        # material type
        ("ambient_medium", material),
        ("temperature", pa.float64()),
        ("pressure", pa.float64()),
        ("potential", pa.float64()),
        ("relative_humidity", pa.float64()),
        # Relationship field (environment -> measurements)
        ("measurement_ids", pa.list_(pa.string())),
        ("timestamp", pa.timestamp("us", tz="UTC")),

    ]
)


# Schema for Reflectivity Model records (refl1d/bumps fit models)
REFLECTIVITY_MODEL_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        #("is_deleted", pa.bool_()),
        # Relationship to reflectivity measurements
        ("measurement_ids", pa.list_(pa.string())),
        # Model identification
        ("model_name", pa.string()),
        ("model_file_path", pa.string()),
        # Software provenance
        ("software", pa.string()),
        ("software_version", pa.string()),
        ("schema_version", pa.string()),
        # Fit summary
        ("num_experiments", pa.int32()),
        pa.field("dataset_index", pa.int32(), metadata={b'description': b'0-based index of the selected dataset in a co-refinement model, null for single-experiment models'}),
        ("num_parameters", pa.int32()),
        ("num_free_parameters", pa.int32()),
        # Layer summary extracted from the selected experiment
        (
            "layers",
            pa.list_(
                pa.struct(
                    [
                        ("layer_number", pa.int32()),
                        ("name", pa.string()),
                        ("thickness", pa.float64()),
                        ("thickness_std", pa.float64()),
                        ("interface", pa.float64()),
                        ("interface_std", pa.float64()),
                        ("sld", pa.float64()),
                        ("sld_std", pa.float64()),
                        ("isld", pa.float64()),
                        ("isld_std", pa.float64()),
                    ]
                )
            ),
        ),
        # Full JSON for reproducibility
        ("model_json", pa.large_string()),
    ],
)


def get_schema_for_model(model_name: str) -> pa.Schema:
    """
    Get the PyArrow schema for a model type.

    Args:
        model_name: One of 'reflectivity', 'sample', 'environment',
                    'reflectivity_model'

    Returns:
        The corresponding PyArrow schema

    Raises:
        ValueError: If model_name is not recognized
    """
    schemas = {
        "reflectivity": REFLECTIVITY_SCHEMA,
        "sample": SAMPLE_SCHEMA,
        "environment": ENVIRONMENT_SCHEMA,
        "reflectivity_model": REFLECTIVITY_MODEL_SCHEMA,
    }
    if model_name not in schemas:
        raise ValueError(f"Unknown model: {model_name}. Expected one of {list(schemas.keys())}")
    return schemas[model_name]
