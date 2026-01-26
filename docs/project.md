We are creating a data-intake workflow for our scientific data lakehouse.

The following are example data files of interest:

    - Processed data to be ingested: $USER/data/REFL_218386_combined_data_auto.txt
    - Raw data that contains metadata: $USER/data/REF_L_218386.nxs.h5
    - Theory model for processed data: $USER/data/expt11-refl1d/Cu-THF-corefine-expt11-1-expt.json

The intent of the workflow is to automate reflectivity data ingestion into our lakehouse as much as possible.
We are ingesting scientific measurements. A scientist first acquires the datta (raw data), which is captured in an hdf5 file.
This data is then processed (we call it 'reduced') to transform the raw data from instrument-space to physics-space.
The text file format of the reduced (physics-space) data may be missing metadata to give context to the measurement.
The reduced (physics-space) data is then modeled (fitted) to a model. The fitted model is saved in a json file.

We want to capture as much information as we can in our data lakehouse to ensure that our data is labelled and AI-ready.

The following are phases for our project!

For each phase, address the goal of the phase, and come up with an executive summary of a plan, and a step-by-step implementation plan.
Put these in markdown files.


Phase 1: Review and Planning

- We have already come up with a data schema for our lakehouse. Review the code in $USER/git/raven_ai. Do not worry about the RavenDB aspect, only concentrate on the models and schema. This will be our target schema. Note that the schema allows for both reflectivity and EIS measurements. We are focused on reflectivity.
- The code in $USER/git/nexus-processor is used to capture all the data in the raw hdf5 file into parquet files. Concentrate on the metadata. This will tell you how to get the missing information. Assume that you can read the parquet files instead of the hdf5 file. The parquet files will also be cataloged in our lakehouse.
- Please plan for an ingest workflow given that information, highlight the missing information, and develop a plan to automate as much as you can.
- Plan for the development of an AI-assistant to help a scientist ingest their data. Design it such that every part that may need user input can easily be replaced by a process/agent/tool in the future.
- The ingested data will be saved as parquet files for Iceberg
