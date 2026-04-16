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

    def __init__(self):
        """
        Initialize the JSON writer.

        """

    def write_reflectivity(self, record: dict[str, Any]) -> str:
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
        db_record["instrument"] = record["instrument"]
        db_record["laboratory"] = record["laboratory"]
        db_record["probe"] = record["probe"]
        db_record["technique"] = record["technique"].capitalize()
        db_record["technique_description"] = record["technique_description"]
        db_record["is_simulated"] = record["is_simulated"]
        db_record["run_title"] = record["run_title"]
        db_record["run_number"] = record["run_number"]
        db_record["run_start"] = record["run_start"].isoformat()
        db_record["raw_file_path"] = record["raw_file_path"]
        db_record["q_1_angstrom"] = record["q_1_angstrom"]
        db_record["r"] = record["r"]
        db_record["d_r"] = record["d_r"]
        db_record["d_q"] = record["d_q"]
        db_record["measurement_geometry"] = record["measurement_geometry"]
        db_record["reduction_time"] = record["reduction_time"].isoformat()
        db_record["reduction_version"] = record["reduction_version"]

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

    def write_sample(self, record: dict[str, Any],env_id) -> str:
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
        db_record["layers"] = record["layers"]
        db_record["substrate"] = record["substrate"]
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

    def write_environment(self, record: dict[str, Any], m_id) -> str:
        """
        Write an environment record to DB.

        Args:
            record: The environment record dict

        Returns:
            Environment ID
        """
        db_record = {}
        db_record["description"] = record["description"]
        db_record["ambient_medium"] = record["ambient_medium"]
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
    def write_all(self, result: AssemblyResult) -> dict[str, str]:
        """
        Write all assembled data to JSON files.

        Args:
            result: The AssemblyResult from DataAssembler

        Returns:
            Dict mapping table names to written file paths
        """
        paths: dict[str, str] = {}
        ref_id = None
        env_id = None
        if result.reflectivity:
            ref_id = self.write_reflectivity(result.reflectivity)
            paths["reflectivity"] = HOSTNAME+'/api/reflectivity/get/'+ref_id
        if result.environment:
            env_id = self.write_environment(result.environment,ref_id)
            paths["environment"] = HOSTNAME+'/api/environment/get/'+env_id
        if result.sample:
            sample_id=self.write_sample(result.sample,env_id)
            paths["sample"] = HOSTNAME+'/api/sample/get/'+sample_id

        return paths


def write_assembly_to_ravendb(result: AssemblyResult) -> dict[str, str]:
    """
    Convenience function to write all results from an assembly to JSON.

    Args:
        result: The AssemblyResult from DataAssembler

    Returns:
        Dict mapping table names to written API paths
    """
    writer = RavenDBWriter()
    return writer.write_all(result)
