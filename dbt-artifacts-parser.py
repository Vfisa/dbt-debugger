import json
import os

from dbt_artifacts_parser.parser import parse_catalog
from dbt_artifacts_parser.parser import parse_manifest
from dbt_artifacts_parser.parser import parse_run_results
from dbt_artifacts_parser.parser import parse_sources

relative_path = os.path.dirname(os.path.realpath(__file__))
print("Local path: "+relative_path)

def get_manifest():
    """
    parse any version of manifest.json
    """
    with open(relative_path+"/target/manifest.json", "r") as fp:
        manifest_dict = json.load(fp)
        manifest_obj = parse_manifest(manifest=manifest_dict)
    return manifest_obj

def get_catalog():
    """
    parse any version of catalog.json
    """
    with open(relative_path+"/target/catalog.json", "r") as fp:
        catalog_dict = json.load(fp)
        catalog_obj = parse_catalog(catalog=catalog_dict)
    return catalog_obj

def get_runresults():
    """
    parse any version of run-results.json
    """
    with open(relative_path+"/target/run_results.json", "r") as fp:
        run_results_dict = json.load(fp)
        run_results_obj = parse_run_results(run_results=run_results_dict)
    return run_results_obj

def get_sources():
    """
    parse any version of run-results.json
    """
    with open(relative_path+"/models/sources.yml", "r") as fp:
        sources_dict = json.load(fp)
        sources_obj = parse_sources(sources=sources_dict)
    return sources_obj

run = get_runresults()
print(run)

