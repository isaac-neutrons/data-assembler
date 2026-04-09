"""
JSON writer for outputting assembled data in AI-ready JSON format.

This module provides functionality to write assembled records to JSON files,
maintaining schema compatibility with the Parquet output for consumers who prefer JSON.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from assembler.workflow import AssemblyResult
import requests

HOSTNAME = "http://0.0.0.0:3000"

class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime, UUID, and Path objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, Path):
            return str(obj)
        return super().default(obj)


class RavenDBWriter:
    """
    Writes assembled data to DB.

    This writer stores the data, by calling the approtiate API calls
    """

    def __init__(self, output_dir: str | Path):
        """
        Initialize the JSON writer.

        Args:
            output_dir: Base directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_reflectivity(self, record: dict[str, Any]) -> Path:
        """
        Write a reflectivity record to DB.

        Args:
            record: The reflectivity record dict

        Returns:
            Reflectivity ID
        """
        db_record = {}
        db_record["proposal_number"] = record["proposal_number"]
        db_record["facility"] = record["facility"]
        db_record["instrument"] = record["instrument_name"]
        db_record["laboratory"] = record["laboratory"]
        db_record["probe"] = record["probe"]
        db_record["technique"] = record["technique"].capitalize()
        db_record["technique_description"] = record["technique_description"]
        db_record["is_simulated"] = record["is_simulated"]
        db_record["run_title"] = record["run_title"]
        db_record["run_number"] = record["run_number"]
        db_record["run_start"] = record["run_start"].isoformat()
        db_record["raw_file_path"] = record["raw_file_path"]
        db_record["q_1_angstrom"] = record["reflectivity"]["q"]
        db_record["r"] = record["reflectivity"]["r"]
        db_record["d_r"] = record["reflectivity"]["dr"]
        db_record["d_q"] = record["reflectivity"]["dq"]
        db_record["measurement_geometry"] = record["reflectivity"]["measurement_geometry"]
        db_record["reduction_time"] = record["reflectivity"]["reduction_time"].isoformat()
        db_record["reduction_version"] = record["reflectivity"]["reduction_version"]

        url=HOSTNAME+"/api/reflectivity/create"
        response = requests.post(url, json=db_record)

        if response.status_code == 201:
            db_object = response.json()
            print(f"Saved Reflectivty Id: {db_object['Id']}")   
            return db_object["Id"]
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response: {response.json()}")
            return None

    def write_sample(self, record: dict[str, Any],env_id) -> Path:
        """
        Write a sample record to DB.

        Args:
            record: The sample record dict

        Returns:
            Sample ID
        """
        db_record = {}
        db_record["description"] = record["description"]
        db_record["environment_ids"] = [env_id]
        db_record["main_composition"] = record["main_composition"]
        db_record["geometry"] = record["geometry"]
        #parse to material and substrate format
        db_layers=[]
        sorted_layers = sorted(record["layers"], key=lambda x: x['layer_number'])
        for layer in sorted_layers:
            db_layer = {}
            db_layer["thickness"] = layer["thickness"]
            db_layer["material"] = {}
            db_layer["material"]["composition"] = layer["material"] 
            db_layer["sld"] =  layer["sld"] 
            db_layer["roughness"] = layer["roughness"] 
            db_layers.append(db_layer)
        db_record["layers"] = db_layers
        #substrate json 
        substrate_json = json.loads(record["substrate_json"])
        db_record["substrate"] = {}
        db_record["substrate"]["thickness"] = substrate_json["thickness"]
        db_record["substrate"]["material"] = {}
        db_record["substrate"]["material"]["composition"] = substrate_json["name"] 
        
        url=HOSTNAME+"/api/sample/create"
        response = requests.post(url, json=db_record)

        if response.status_code == 201:
            # print(f"Status Code: {response.status_code}")
            db_object = response.json()
            #print(f"Saved Object: {db_object}") 
            print(f"Saved Sample Id: {db_object['Id']}") 
            return db_object["Id"]
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response: {response.json()}")
            return None

    def write_environment(self, record: dict[str, Any], m_id) -> Path:
        """
        Write an environment record to DB.

        Args:
            record: The environment record dict

        Returns:
            Environment ID
        """

        #print("record",record.keys())
        db_record = {}
        db_record["description"] = record["description"]
        db_record["ambient_medium"] = {
            "composition":record["ambient_medium"]
        }
        db_record["temperature"] = record["temperature"]
        db_record["pressure"] = record["pressure"]
        db_record["potential"] = record["potential"]
        db_record["relative_humidity"] = record["relative_humidity"]
        db_record["measurements_ids"] =  [m_id]

        url=HOSTNAME+"/api/environment/create"
        response = requests.post(url, json=db_record)

        if response.status_code == 201:
            # print(f"Status Code: {response.status_code}")
            db_object = response.json()
            print(f"Saved Environment Id: {db_object['Id']}") 
            return db_object["Id"]
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response: {response.json()}")
            return None
    def write_all(self, result: AssemblyResult) -> dict[str, Path]:
        """
        Write all assembled data to JSON files.

        Args:
            result: The AssemblyResult from DataAssembler

        Returns:
            Dict mapping table names to written file paths
        """
        paths: dict[str, Path] = {}
        ref_id = None
        env_id = None
        if result.reflectivity:
            ref_id = self.write_reflectivity(result.reflectivity)

        if result.environment:
            env_id = self.write_environment(result.environment,ref_id)

        if result.sample:
            self.write_sample(result.sample,env_id)


        return paths


def write_assembly_to_ravendb(result: AssemblyResult, output_dir: str | Path) -> dict[str, Path]:
    """
    Convenience function to write all results from an assembly to JSON.

    Args:
        result: The AssemblyResult from DataAssembler
        output_dir: Directory for output files

    Returns:
        Dict mapping table names to written file paths
    """
    print("RavenDBWriter")
    writer = RavenDBWriter(output_dir)
    return writer.write_all(result)
