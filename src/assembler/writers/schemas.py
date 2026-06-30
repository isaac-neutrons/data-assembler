"""
PyArrow schema definitions for lakehouse tables.

These schemas define the Parquet file structure for each model type,
ensuring compatibility with Apache Iceberg tables.
"""

import pyarrow as pa

material = pa.struct(
    [
        ("name", pa.string()),
        ("mass", pa.float64()),
        ("density", pa.float64()),
        ("sld", pa.float64()),  # rho
        ("isld", pa.float64()),  # irho
    ]
)

layer = pa.struct(
    [
        ("name", pa.string()),
        ("material", material),
        ("thickness", pa.float64()),
        ("roughness", pa.float64()),
    ]
)

# Electrolyte/solution for electrochemical environments
electrolyte = pa.struct(
    [
        ("name", pa.string()),
        ("concentration_M", pa.float64()),
    ]
)

# Per-layer fitted parameters (thickness/SLD/roughness + their fitted sigma).
# Shared by the fit record's top-level ``layers`` (primary dataset) and each
# entry of its per-dataset ``datasets`` list.
layer_fit = pa.struct(
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

# Schema for Reflectivity measurements
REFLECTIVITY_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        # Measurement fields
        ("proposal_number", pa.string()),
        ("facility", pa.string()),
        ("laboratory", pa.string()),
        ("probe", pa.string()),
        ("technique", pa.string()),
        ("technique_description", pa.string()),
        pa.field(
            "is_simulated",
            pa.bool_(),
            metadata={b"description": b"Indicates if the measurement is simulated data"},
        ),
        ("run_title", pa.string()),
        ("run_number", pa.string()),
        ("run_start", pa.timestamp("us", tz="UTC")),
        ("raw_file_path", pa.string()),
        ("instrument_name", pa.string()),
        # Reflectivity-specific fields
        ("measurement_geometry", pa.string()),
        ("reduction_time", pa.timestamp("us", tz="UTC")),
        ("reduction_version", pa.string()),
        pa.field("q", pa.list_(pa.float64()), metadata={b"units": b"the units"}),
        ("r", pa.list_(pa.float64())),
        ("dr", pa.list_(pa.float64())),
        ("dq", pa.list_(pa.float64())),
        # Relationship fields (run -> sample/environment). Nullable foreign keys
        # kept at the run level: a "state" (one physical condition measured at
        # several angles) is derivable as runs sharing (sample_id,
        # environment_id) without any grouping column in this table.
        ("sample_id", pa.string()),
        ("environment_id", pa.string()),
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
        # Relationship to fit records constraining this sample (forward lookup).
        ("fit_ids", pa.list_(pa.string())),
        # Layers as JSON string (nested struct alternative)
        # ("layers_json", pa.string()),
        (
            "layers",
            pa.list_(
                # layers are already sorted
                layer
            ),
        ),
        # subrater is one layer
        ("substrate", layer),
        # for publications
        ("publication_ids", pa.list_(pa.string())),
    ]
)

# Schema for Environment records
ENVIRONMENT_SCHEMA = pa.schema(
    [
        # Base DataModel fields
        ("id", pa.string()),
        ("created_at", pa.timestamp("us", tz="UTC")),
        # Environment fields
        ("description", pa.string()),
        # material type
        ("ambient_medium", material),
        ("temperature", pa.float64()),
        ("pressure", pa.float64()),
        ("relative_humidity", pa.float64()),
        # Electrochemical conditions (operando/in-situ)
        pa.field(
            "potential",
            pa.float64(),
            metadata={b"description": b"Applied potential setpoint on the scale named by potential_scale"},
        ),
        ("potential_scale", pa.string()),
        pa.field(
            "control_mode",
            pa.string(),
            metadata={b"description": b"e.g. open_circuit, potentiostatic, galvanostatic"},
        ),
        ("electrolyte", electrolyte),
        ("pH", pa.float64()),
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
        # Relationship to reflectivity measurements. Populated with ALL runs the
        # fit used (one per dataset in a co-refinement), making the fit a
        # first-class entity that links every run it constrains.
        ("measurement_ids", pa.list_(pa.string())),
        # Relationship to sample(s) the fit constrains.
        ("sample_id", pa.string()),
        ("sample_ids", pa.list_(pa.string())),
        # Fitting strategy + tied/free parameter assumptions (co-refinement).
        pa.field(
            "fit_strategy",
            pa.string(),
            metadata={b"description": b"single | single_state_coref | multi_state_coref"},
        ),
        ("shared_parameters", pa.list_(pa.string())),
        ("unshared_parameters", pa.list_(pa.string())),
        # Model identification
        ("model_name", pa.string()),
        ("model_file_path", pa.string()),
        # Software provenance
        ("software", pa.string()),
        ("software_version", pa.string()),
        ("schema_version", pa.string()),
        # Fit summary
        ("num_experiments", pa.int32()),
        pa.field(
            "dataset_index",
            pa.int32(),
            metadata={
                b"description": b"0-based index of the selected dataset in a co-refinement model, null for single-experiment models"
            },
        ),
        ("num_parameters", pa.int32()),
        ("num_free_parameters", pa.int32()),
        pa.field(
            "chi_squared",
            pa.float64(),
            metadata={b"description": b"Reduced chi-squared goodness-of-fit of the model"},
        ),
        # Layer summary for the primary/selected dataset (mirrors
        # datasets[primary].layers; kept top-level for ISAAC-writer back-compat).
        ("layers", pa.list_(layer_fit)),
        # Per-dataset fitted parameters: one entry per run the fit used, so a
        # co-refinement is a complete, self-contained, queryable AI-ready record.
        (
            "datasets",
            pa.list_(
                pa.struct(
                    [
                        ("dataset_index", pa.int32()),
                        ("measurement_id", pa.string()),
                        ("run_number", pa.string()),
                        ("chi_squared", pa.float64()),
                        ("layers", pa.list_(layer_fit)),
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
        # Alias: the reflectivity_model record is the first-class "fit" entity
        # (links all its runs, carries per-dataset params); new code may use
        # either name.
        "fit": REFLECTIVITY_MODEL_SCHEMA,
    }
    if model_name not in schemas:
        raise ValueError(f"Unknown model: {model_name}. Expected one of {list(schemas.keys())}")
    return schemas[model_name]
