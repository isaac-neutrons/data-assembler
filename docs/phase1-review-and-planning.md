# Phase 1: Review and Planning

## Executive Summary

This document outlines the plan for building an automated reflectivity data ingestion workflow for the scientific data lakehouse. The workflow will ingest three types of interconnected data:

1. **Raw Data (HDF5/NeXus)** - Contains rich metadata from instrument acquisition
2. **Reduced Data (Text)** - Physics-space measurements lacking metadata context  
3. **Model Data (JSON)** - Fitted theoretical models with sample layer structures

The key challenge is **metadata reconciliation**: the reduced data text file lacks critical context that must be retrieved from the raw HDF5 file (via parquet intermediaries). The workflow must link these data sources, enrich the reduced data with metadata, and structure everything according to the target lakehouse schema.

### Key Findings

| Component | Source | Target Schema | Gap Analysis |
|-----------|--------|---------------|--------------|
| Measurement metadata | HDF5 → Parquet | `Measurement`, `Reflectivity` | Run title, proposal, timing available in parquet |
| Sample information | HDF5 → Parquet | `Sample`, `Layer`, `Material` | Chemical formula, name, nature in sample.parquet |
| Users/Team | HDF5 → Parquet | (to be designed) | User names, roles, facility IDs available |
| Reflectivity data | Text file | `Reflectivity` | Q, R, dR, dQ columns directly map |
| Layer structure | Model JSON | `Sample.layers`, `Layer` | Refl1d model has detailed layer/material info |
| Environment | HDF5 → Parquet (DASlogs) | `Environment` | Temperature, pressure from instrument logs |

### Automation Potential

| Task | Automation Level | Notes |
|------|------------------|-------|
| Extract metadata from parquet | **Fully Automated** | Structured schema available |
| Parse reduced data file | **Fully Automated** | Well-defined text format |
| Parse model JSON | **Fully Automated** | Structured bumps/refl1d schema |
| Link raw file to reduced file | **Semi-Automated** | Run number matching; may need confirmation |
| Map sample layers to schema | **AI-Assisted** | Layer names may need interpretation |
| Map material composition | **AI-Assisted** | Chemical formula extraction/validation |
| Create environment record | **AI-Assisted** | Select relevant DASlogs for environment |

---

## Target Schema Analysis (raven_ai)

### Core Data Models

#### Measurement (Base Class)
```python
class Measurement(DataModel): 
    proposal_number: str          # From metadata.parquet → experiment_identifier (IPTS-XXXXX)
    facility: str                 # Enum: SNS, HFIR, LCLS - from instrument name
    lab: str                      # Enum: SLAC - from facility
    probe: str                    # Enum: neutrons, xray, other - from instrument
    technique: str                # "reflectivity" for REF_L
    technique_description: str    # Prepend technique value 
    is_simulated: bool            # Default False
    run_title: str                # From metadata.parquet → title
    run_number: str               # From metadata.parquet → run_number
    run_start: datetime           # From metadata.parquet → start_time
    raw_file_path: str            # From metadata.parquet → source_path
```

#### Reflectivity (Extends Measurement)
```python
class Reflectivity(Measurement):    
    q_1_angstrom: float           # From reduced text file (column 1)
    r: float                      # From reduced text file (column 2)  
    dR: float                     # From reduced text file (column 3)
    dQ: float                     # From reduced text file (column 4)
    measurement_geometry: float   # From reduced text header (TwoTheta)
    reduction_time: datetime      # From reduced text header (Reduction time)
```

#### Sample
```python
class Sample(DataModel):
    description: str              # From run_title or sample.parquet → nature
    environment_ids: list[str]    # Links to Environment documents
    substrate: Layer              # Bottom layer from model JSON (Si)
    main_composition: str         # Primary material chemical formula
    geometry: str                 # Optional, from substrate
    layers: list[Layer]           # From model JSON layer stack
```

#### Layer & Material
```python
class Layer(BaseModel):
    material: Material            # SLD and composition
    thickness: float              # From model JSON (Angstroms)

class Material(BaseModel):
    composition: str              # Chemical formula (Cu, Ti, THF, Si)
    mass: float                   # Optional
    density: float                # Can compute from rho values
```

#### Environment
```python
class Environment(DataModel):
    description: str              # Experimental condition description
    ambiant_medium: Material      # e.g., THF solvent
    temperature: float            # From DASlogs (SampleTemp)
    pressure: float               # From DASlogs if available
    relative_humidity: float      # From DASlogs if available
    measurements_ids: list[str]   # Links to Measurement documents
```

---

## Data Source Analysis

### 1. Raw Data: HDF5/NeXus → Parquet Files

The `nexus-processor` extracts HDF5 data into these parquet schemas:

#### metadata.parquet
| Field | Type | Maps To |
|-------|------|---------|
| `instrument_id` | string | → facility/instrument detection |
| `run_number` | int64 | → `Measurement.run_number` |
| `title` | string | → `Measurement.run_title` |
| `start_time` | string (ISO) | → `Measurement.run_start` |
| `end_time` | string (ISO) | duration calculation |
| `experiment_identifier` | string | → `Measurement.proposal_number` (IPTS-XXXXX) |
| `source_path` | string | → `Measurement.raw_file_path` |
| `proton_charge` | float | instrument health metric |
| `total_counts` | int64 | data quality metric |

#### sample.parquet
| Field | Type | Maps To |
|-------|------|---------|
| `name` | string | → `Sample.description` |
| `nature` | string | → sample type classification |
| `chemical_formula` | string | → `Sample.main_composition` |
| `mass` | float | → `Material.mass` |
| `temperature` | float | → `Environment.temperature` |
| `additional_fields` | map | flexible extension |

#### users.parquet
| Field | Type | Maps To |
|-------|------|---------|
| `name` | string | team member name |
| `facility_user_id` | string | facility ID |
| `role` | string | experimenter role |

#### daslogs.parquet (Environment Data)
| Field | Type | Relevant Logs |
|-------|------|---------------|
| `log_name` | string | `SampleTemp`, `pressure`, `humidity` |
| `average_value` | float | steady-state value |
| `min_value` | float | variation tracking |
| `max_value` | float | variation tracking |

### 2. Reduced Data: Text File Format

From `REFL_218386_combined_data_auto.txt`:

```
# Experiment IPTS-34347 Run 218386
# Reduction 2.2.0.dev47+d202502071725
# Run title: CuPt_d8-THF_FullQ-218386-1.
# Run start time: 2025-04-20T13:42:43.100443667
# Reduction time: Sun Apr 20 09:45:26 2025
# Q summing: False
# TOF weighted: False
# Bck in Q: False
# Theta offset: 0
# DataRun   NormRun   TwoTheta(deg)  LambdaMin(A)   LambdaMax(A) Qmin(1/A)    Qmax(1/A)    SF_A         SF_B
# 218386    218274    0.899957       2.74977        9.49868      0.0103       0.0358       4.0          0           
# Q [1/Angstrom]        R                     dR                    dQ [FWHM]            
  0.0104865232866460    1.0118753474992772    0.0334283255280669    0.0002849861112957
  ...
```

**Extractable Fields:**
- `proposal_number`: "IPTS-34347"
- `run_number`: "218386" 
- `run_title`: "CuPt_d8-THF_FullQ-218386-1."
- `run_start`: "2025-04-20T13:42:43.100443667"
- `reduction_time`: "Sun Apr 20 09:45:26 2025"
- Reduction parameters (for provenance)
- Data columns: Q, R, dR, dQ

**Key Insight:** The reduced file contains enough info to LINK to the raw file via run_number and IPTS.

### 3. Model Data: Refl1d JSON Format

The model JSON contains:
- **Sample Stack Structure**: Layer sequence with materials
- **Material Properties**: SLD (rho), absorption (irho)
- **Layer Parameters**: Thickness, interface roughness
- **Probe Data**: Q, R, dR, dQ arrays (duplicate of reduced data)
- **Fit Parameters**: Fitted vs fixed values, bounds

**Sample Layer Extraction:**
```
Stack (top to bottom):
  - THF (thickness: 0 Å, rho: 5.88) - ambient/solvent
  - material (thickness: 37.4 Å, rho: 4.88) - film layer
  - Cu (thickness: 481.9 Å, rho: 6.31) - conductor
  - Ti (thickness: 36.7 Å, rho: -1.35) - adhesion
  - Si (thickness: 0 Å, rho: 2.07) - substrate
```

---

## Output Format: Parquet for Apache Iceberg

The ingested data will be written as **Parquet files** for storage in an **Apache Iceberg** data lakehouse. This has implications for schema design and type mapping.

### Parquet Type Mapping

| Python Type | Parquet Type | Notes |
|-------------|--------------|-------|
| `str` | `string` | UTF-8 encoded |
| `int` | `int64` | 64-bit integers |
| `float` | `double` | 64-bit floating point |
| `bool` | `boolean` | |
| `datetime` | `timestamp[us, tz=UTC]` | Microsecond precision with timezone |
| `list[float]` | `list<double>` | For Q/R/dR/dQ arrays |
| `list[str]` | `list<string>` | For ID references |
| `Enum` | `string` | Serialize as enum value string |
| `Optional[T]` | nullable T | Parquet supports nullable columns |
| Nested `BaseModel` | `struct` | For Layer, Material embedded objects |
| `dict` | JSON `string` | Complex dicts serialized as JSON |

### Iceberg Table Structure

We will create separate Iceberg tables for each entity type:

```
lakehouse/
├── measurements/           # Measurement & Reflectivity records
│   ├── metadata/          # Iceberg metadata
│   └── data/              # Parquet files partitioned by facility/year
├── samples/               # Sample records with embedded layers
│   ├── metadata/
│   └── data/
├── environments/          # Environment records
│   ├── metadata/
│   └── data/
└── materials/             # Material reference table (optional)
    ├── metadata/
    └── data/
```

### Partitioning Strategy

- **Measurements**: Partition by `facility` and `year(run_start)`
- **Samples**: Partition by `year(created_at)`
- **Environments**: Partition by `year(created_at)`

### Schema Evolution

Iceberg supports schema evolution (adding columns, widening types). Our Pydantic models should:
1. Use `Optional` for new fields to maintain backward compatibility
2. Avoid removing or renaming fields without migration
3. Document schema version in metadata

---

## Schema Alignment with raven_ai

The data-assembler models are **reimplemented** (not imported from raven_ai) for the following reasons:

1. **No external dependency** - data-assembler is self-contained
2. **Optimized for ingestion** - Models tailored for parsing and transformation
3. **Parquet-first design** - Types chosen for efficient Parquet serialization
4. **Independent evolution** - Can add ingestion-specific fields without affecting raven_ai

### Compatibility Approach

```
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│   data-assembler    │      │   Parquet/Iceberg   │      │      raven_ai       │
│   (Pydantic v2)     │ ───▶ │   (Column Schema)   │ ◀─── │   (Pydantic v2)     │
│   Ingestion Models  │      │   Source of Truth   │      │   Query Models      │
└─────────────────────┘      └─────────────────────┘      └─────────────────────┘
```

The **Parquet column schema** is the source of truth. Both data-assembler and raven_ai must:
- Use the same field names
- Use compatible types (after serialization)
- Agree on required vs optional fields

### Type Differences from raven_ai

| Field | raven_ai Type | data-assembler Type | Reason |
|-------|---------------|---------------------|--------|
| `q`, `r`, `dR`, `dQ` | `float` (single values) | `list[float]` | Parquet stores full arrays; raven_ai was per-point |
| `facility`, `probe`, `technique` | `str` | `Enum` (serializes to `str`) | Type safety during ingestion |
| `layers` | `list[Layer]` | `list[Layer]` | Same - Parquet uses `list<struct>` |
| `Id` (raven_ai) | `Optional[str]` | `id` (our base) | Field name normalization |
| `reduction_parameters` | N/A | `Optional[dict]` | New field for provenance |

### Validation with raven_ai (Optional)

For production deployments, raven_ai can be installed as an optional dependency to validate output compatibility:

```python
# With raven_ai installed:
from raven_ai.models import Reflectivity as RavenReflectivity

def validate_compatibility(our_record: dict) -> bool:
    """Validate that our output is compatible with raven_ai schema."""
    try:
        RavenReflectivity(**our_record)
        return True
    except ValidationError as e:
        logger.warning(f"Schema compatibility issue: {e}")
        return False
```

---

## Gap Analysis: Missing Information

### Data Available in Parquet but NOT in Reduced File:
1. ✅ Full experiment metadata
2. ✅ Sample temperature and environment conditions  
3. ✅ User/team information
4. ✅ Instrument configuration
5. ✅ Normalization run details

### Data Available in Model JSON but NOT in Raw Data:
1. ✅ Fitted layer structure with materials
2. ✅ Material SLD values
3. ✅ Interface roughness values
4. ✅ Fit quality metrics

### Data Requiring Human/AI Input:
1. ⚠️ **Material Composition**: SLD → chemical formula mapping
2. ⚠️ **Layer Naming**: "material" layer needs proper identification
3. ⚠️ **Environment Description**: Context for measurement conditions
4. ⚠️ **Sample Geometry**: Not explicitly stored
5. ⚠️ **Related Publications**: External linking

---

## Proposed Ingest Workflow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION WORKFLOW                          │
└─────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
    │   Raw HDF5   │     │ Reduced TXT  │     │  Model JSON  │
    │   (.nxs.h5)  │     │   (Q,R,dR)   │     │  (refl1d)    │
    └──────┬───────┘     └──────┬───────┘     └──────┬───────┘
           │                    │                    │
           ▼                    │                    │
    ┌──────────────┐           │                    │
    │nexus-processor│           │                    │
    │ (HDF5→Parquet)│           │                    │
    └──────┬───────┘           │                    │
           │                    │                    │
           ▼                    ▼                    ▼
    ┌──────────────────────────────────────────────────────────┐
    │                    ASSEMBLER SERVICE                      │
    │  ┌────────────────────────────────────────────────────┐  │
    │  │              1. FILE DETECTION & LINKING            │  │
    │  │  • Detect input file type                          │  │
    │  │  • Extract run_number, IPTS                        │  │
    │  │  • Find related files (parquet, model)             │  │
    │  └────────────────────────────────────────────────────┘  │
    │                          │                               │
    │                          ▼                               │
    │  ┌────────────────────────────────────────────────────┐  │
    │  │              2. METADATA EXTRACTION                 │  │
    │  │  • Parse metadata.parquet → Measurement fields     │  │
    │  │  • Parse sample.parquet → Sample fields            │  │
    │  │  • Parse daslogs.parquet → Environment fields      │  │
    │  │  • Parse users.parquet → Team information          │  │
    │  └────────────────────────────────────────────────────┘  │
    │                          │                               │
    │                          ▼                               │
    │  ┌────────────────────────────────────────────────────┐  │
    │  │              3. REDUCED DATA PARSING                │  │
    │  │  • Parse header comments for metadata              │  │
    │  │  • Extract Q, R, dR, dQ columns                    │  │
    │  │  • Extract reduction parameters                    │  │
    │  └────────────────────────────────────────────────────┘  │
    │                          │                               │
    │                          ▼                               │
    │  ┌────────────────────────────────────────────────────┐  │
    │  │              4. MODEL PARSING                       │  │
    │  │  • Parse bumps/refl1d JSON schema                  │  │
    │  │  • Extract layer stack (materials, thickness)      │  │
    │  │  • Extract fit parameters and quality              │  │
    │  └────────────────────────────────────────────────────┘  │
    │                          │                               │
    │                          ▼                               │
    │  ┌────────────────────────────────────────────────────┐  │
    │  │         5. SCHEMA MAPPING & ENRICHMENT             │  │
    │  │  ┌─────────────────────────────────────────────┐   │  │
    │  │  │           AI-ASSISTED TASKS                 │   │  │
    │  │  │  • Material identification (SLD→formula)   │   │  │
    │  │  │  • Layer naming clarification              │   │  │
    │  │  │  • Environment description                 │   │  │
    │  │  │  • Data quality validation                 │   │  │
    │  │  └─────────────────────────────────────────────┘   │  │
    │  └────────────────────────────────────────────────────┘  │
    │                          │                               │
    │                          ▼                               │
    │  ┌────────────────────────────────────────────────────┐  │
    │  │              6. VALIDATION & REVIEW                 │  │
    │  │  • Schema validation (Pydantic)                    │  │
    │  │  • Consistency checks                              │  │
    │  │  • User confirmation (optional)                    │  │
    │  └────────────────────────────────────────────────────┘  │
    │                          │                               │
    └──────────────────────────┼───────────────────────────────┘
                               │
                               ▼
                    ┌──────────────────┐
                    │   DATA LAKEHOUSE  │
                    │   (Iceberg/Parquet)│
                    │  ┌────────────┐  │
                    │  │ Measurement │  │
                    │  │ Sample      │  │
                    │  │ Environment │  │
                    │  │ Material    │  │
                    │  └────────────┘  │
                    └──────────────────┘
```

---

## AI-Assistant Design

### Architecture: Modular & Replaceable Components

```
┌──────────────────────────────────────────────────────────────────┐
│                    AI INGESTION ASSISTANT                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐ │
│  │  INPUT HANDLERS │    │   PROCESSORS    │    │   OUTPUTS    │ │
│  │  (Replaceable)  │    │  (Replaceable)  │    │ (Replaceable)│ │
│  ├─────────────────┤    ├─────────────────┤    ├──────────────┤ │
│  │ • FileDetector  │───▶│ • MetadataExtractor │▶│ • Validator  │ │
│  │ • ParquetReader │    │ • ReducedParser │    │ • Formatter  │ │
│  │ • JSONParser    │    │ • ModelParser   │    │ • DBWriter   │ │
│  │ • TextParser    │    │ • SchemaMapper  │    │ • Reporter   │ │
│  └─────────────────┘    │ • AIEnricher    │    └──────────────┘ │
│                         └─────────────────┘                      │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              INTERACTION LAYER (Replaceable)                 │ │
│  ├─────────────────────────────────────────────────────────────┤ │
│  │  Interface Options:                                          │ │
│  │  • Human User (CLI/Web UI)                                  │ │
│  │  • LLM Agent (tool-calling)                                 │ │
│  │  • Automated Pipeline (no interaction)                      │ │
│  │  • Approval Workflow (human-in-the-loop)                    │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              DECISION POINTS (AI-Assisted)                   │ │
│  ├─────────────────────────────────────────────────────────────┤ │
│  │  • Material identification from SLD values                  │ │
│  │  • Layer naming when name is generic (e.g., "material")     │ │
│  │  • Environment description generation                       │ │
│  │  • Anomaly detection in data                                │ │
│  │  • Missing data resolution                                  │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

### Tool Definitions for AI Agent

Each component should be exposed as a **tool** that can be called by:
- Human operator via CLI/UI
- LLM agent via function calling
- Automated pipeline via direct API

#### Tool: `detect_file_type`
```python
def detect_file_type(file_path: str) -> FileInfo:
    """
    Detect the type of input file and extract basic identifiers.
    
    Returns: FileInfo with type (reduced/raw/model), run_number, ipts
    """
```

#### Tool: `find_related_files`
```python
def find_related_files(run_number: str, ipts: str, search_paths: list[str]) -> RelatedFiles:
    """
    Find parquet files, reduced data, and model files for a given run.
    
    Returns: RelatedFiles with paths to metadata.parquet, sample.parquet, etc.
    """
```

#### Tool: `extract_measurement_metadata`
```python
def extract_measurement_metadata(parquet_paths: dict) -> MeasurementDraft:
    """
    Extract measurement fields from parquet files.
    
    Returns: Draft Measurement object with populated fields
    """
```

#### Tool: `parse_reduced_data`
```python
def parse_reduced_data(file_path: str) -> ReducedData:
    """
    Parse reduced reflectivity text file.
    
    Returns: ReducedData with header info and Q/R/dR/dQ arrays
    """
```

#### Tool: `parse_model`
```python  
def parse_model(file_path: str) -> ModelData:
    """
    Parse refl1d/bumps model JSON.
    
    Returns: ModelData with layer stack, materials, parameters
    """
```

#### Tool: `identify_material`
```python
def identify_material(sld: float, name_hint: str = None) -> Material:
    """
    AI-assisted: Identify material composition from SLD value.
    May use periodictable library or AI reasoning.
    
    Returns: Material with composition, density
    """
```

#### Tool: `describe_environment`
```python
def describe_environment(daslogs: dict, sample_info: dict) -> str:
    """
    AI-assisted: Generate human-readable environment description.
    
    Returns: Description string for Environment.description
    """
```

#### Tool: `validate_and_create`
```python
def validate_and_create(draft: IngestDraft) -> ValidationResult:
    """
    Validate draft against schema and create records.
    
    Returns: ValidationResult with success/errors and created IDs
    """
```

---

## Step-by-Step Implementation Plan

### Phase 1A: Core Infrastructure (Week 1-2)

1. **Set up project structure**
   ```
   data-assembler/
   ├── src/
   │   ├── assembler/
   │   │   ├── __init__.py
   │   │   ├── models/           # Pydantic models matching raven_ai
   │   │   ├── parsers/          # File parsers
   │   │   │   ├── parquet_parser.py
   │   │   │   ├── reduced_parser.py
   │   │   │   └── model_parser.py
   │   │   ├── tools/            # Agent-callable tools
   │   │   ├── workflow/         # Orchestration logic
   │   │   └── ai/               # AI-assisted components
   │   └── cli/                  # Command-line interface
   ├── tests/
   └── docs/
   ```

2. **Implement Pydantic models** matching raven_ai schema
   - `Measurement`, `Reflectivity`
   - `Sample`, `Layer`, `Material`
   - `Environment`
   - Draft/intermediate models for workflow

3. **Implement file parsers**
   - Parquet reader for nexus-processor output
   - Reduced text file parser
   - Refl1d JSON model parser

### Phase 1B: Workflow Logic (Week 2-3)

4. **Implement file detection and linking**
   - Run number extraction from filenames/content
   - IPTS extraction from metadata
   - Related file discovery in configured paths

5. **Implement schema mapping**
   - Parquet → Measurement fields
   - Reduced text → Reflectivity data
   - Model JSON → Sample layers

6. **Implement validation layer**
   - Schema validation (Pydantic)
   - Cross-reference validation
   - Data quality checks
   - Optional raven_ai compatibility validation

7. **Implement Parquet/Iceberg writer**
   - Convert Pydantic models to PyArrow schema
   - Write partitioned Parquet files
   - Iceberg table registration (if catalog available)

### Phase 1C: AI Integration (Week 3-4)

8. **Implement AI-assisted tools**
   - Material identification from SLD
   - Environment description generation
   - Layer naming clarification

9. **Implement interaction layer**
   - CLI for manual operation
   - Tool definitions for LLM agent
   - Approval workflow hooks

### Phase 1D: Integration & Testing (Week 4-5)

10. **End-to-end testing**
   - Test with example files (218386 dataset)
   - Validate output against raven_ai schema
   - Performance benchmarking

11. **Documentation**
    - API documentation
    - User guide
    - AI agent integration guide
    - Schema compatibility guide

---

## Success Criteria

1. **Functional**: Successfully ingest the 218386 example dataset
2. **Complete**: All schema fields populated with appropriate values
3. **Accurate**: Metadata correctly linked between sources
4. **Modular**: Each component replaceable independently
5. **AI-Ready**: Tools can be called by LLM agent
6. **Validated**: All outputs pass Pydantic validation

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Missing parquet files for run | Cannot get full metadata | Fall back to reduced file header; flag incomplete |
| Ambiguous material identification | Incorrect composition | AI-assisted with human confirmation option |
| Schema version mismatch | Data incompatibility | Version-aware parsers; migration support |
| Large file processing | Memory/time issues | Streaming/chunked processing for events |

---

## Next Steps

1. ✅ Complete this planning document
2. 🔲 Set up project structure
3. 🔲 Implement core Pydantic models
4. 🔲 Implement parquet parser
5. 🔲 Implement reduced text parser
6. 🔲 Implement model JSON parser
7. 🔲 Create end-to-end workflow
8. 🔲 Add AI-assisted tools
9. 🔲 Test with example data
10. 🔲 Document and deploy
