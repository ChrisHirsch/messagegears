import argparse
import configparser
import json
import requests
import yaml
import os

# Constants for API endpoints
ENDPOINTS = {
    "connections": "beta/audience/connection",
    "templates": "beta/template",
    "audiences": "beta/audience/query",
}

# Helper Functions
def get_endpoint(base_url, key):
    """Construct a full API endpoint URL."""
    return f"{base_url}{ENDPOINTS[key]}"

def send_request(method, url, headers=None, json=None, params=None):
    """Send an HTTP request and handle errors."""
    try:
        response = requests.request(method, url, headers=headers, json=json, params=params)
        response.raise_for_status()
        return response.json()  # Return JSON response if successful
    except requests.exceptions.RequestException as e:
        print(f"HTTP error: {e}")
    except ValueError:
        print("Response could not be decoded as JSON.")
    return None

def prepare_headers(config, key="source"):
    """Prepare headers for API requests."""
    section = config.get(key, {})
    return {
        "ApiKey": section.get("ApiKey"),
        "CustomerId": section.get("CustomerId"),
        "Content-Type": "application/json",
    }

def validate_config(config):
    """Validate the required configuration fields."""
    required_sections = ["source", "destination"]
    required_fields = {
        "source": ["url", "ApiKey", "CustomerId"],
        "destination": ["url", "ApiKey", "CustomerId"],
    }

    for section in required_sections:
        if section not in config:
            raise KeyError(f"Missing '{section}' section in configuration.")
        for field in required_fields[section]:
            if field not in config[section]:
                raise KeyError(f"Missing '{field}' in '{section}' section.")

def fetch_paginated_data(config, endpoint, params=None):
    """Fetch all data from a paginated API."""
    url = get_endpoint(config['source']['url'], endpoint)
    headers = prepare_headers(config)
    page = 1
    size = params.get("size", 100) if params else 100
    all_content = []

    while True:
        current_params = {"page": page - 1, "size": size}
        if params:
            current_params.update(params)

        data = send_request("GET", url, headers=headers, params=current_params)
        if not data:
            break

        all_content.extend(data.get("content", []))
        if data.get("last", False):
            break

        page += 1
    return all_content

def sync_connections(db_payload, config):
    """Synchronize database connections by comparing source and configuration."""
    config_databases = config.get("databases", [])

    source_map = {db["name"].lower(): db for db in db_payload}
    config_map = {db["name"].lower(): db for db in config_databases}

    common_names = source_map.keys() & config_map.keys()
    headers = prepare_headers(config, key="destination")
    api_url = get_endpoint(config['destination']['url'], "connections")

    for name in common_names:
        source_db = source_map[name]
        config_db = config_map[name]

        payload = {
            **source_db,
            "password": config_db.get("password"),
            "port": config_db.get("port"),
            "host": config_db.get("host"),
            "user": config_db.get("user"),
        }
        response = send_request("POST", api_url, headers=headers, json=payload)
        if response:
            print(f"Successfully created database connection for '{name}'")
        else:
            print(f"Failed to create database connection for '{name}'")

def fetch_content(config, template_id):
    """Fetch a single template by ID."""
    url = get_endpoint(config['source']['url'], "templates") + f"/{template_id}"
    headers = prepare_headers(config)
    return send_request("GET", url, headers=headers)

def create_template(config, template_data):
    """Create a template in the destination system."""
    url = get_endpoint(config['destination']['url'], "templates")
    headers = prepare_headers(config, key="destination")
    response = send_request("POST", url, headers=headers, json=template_data)

    if response:
        print("Template created successfully.")
        return response
    else:
        print("Failed to create template.")
    return None

def fetch_audiences(config):
    """Fetch all audiences from the source system."""
    print("Fetching audiences from source...")
    return fetch_paginated_data(config, "audiences")

def create_audience(config, audience_data):
    """Create an audience in the destination system."""
    url = get_endpoint(config['destination']['url'], "audiences")
    headers = prepare_headers(config, key="destination")
    response = send_request("POST", url, headers=headers, json=audience_data)

    if response:
        print(f"Audience '{audience_data['name']}' created successfully.")
        return response
    else:
        print(f"Failed to create audience '{audience_data['name']}': {response}")
    return None

def load_config(file_path):
    """Load configuration from a file (INI, JSON, YAML)."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")

    _, ext = os.path.splitext(file_path)
    ext = ext.lower()

    if ext == ".ini":
        return load_ini(file_path)
    elif ext == ".json":
        return load_json(file_path)
    elif ext in [".yaml", ".yml"]:
        return load_yaml(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}. Supported types are: INI, JSON, YAML.")

def load_ini(file_path):
    """Load INI configuration file."""
    config = configparser.ConfigParser()
    config.read(file_path)
    return {section: dict(config[section]) for section in config.sections()}

def load_json(file_path):
    """Load JSON configuration file."""
    with open(file_path, "r") as f:
        return json.load(f)

def load_yaml(file_path):
    """Load YAML configuration file."""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Read and display configuration file values.")
    parser.add_argument(
        "-c", "--config",
        required=True,
        help="Path to the configuration file (INI, JSON, or YAML)."
    )
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    config = load_config(args.config)
    validate_config(config)

    # Check migration flags in config
    migrate_config = config.get("migrating", {})

    if migrate_config.get("templates", False):
        print("Fetching templates...")
        templates = fetch_paginated_data(config, "templates")
        for template in templates:
            print(f"Migrating Template ID: {template['id']}")
            template_data = fetch_content(config, template['id'])
            if template_data:
                create_template(config, template_data)

    if migrate_config.get("audiences", False):
        print("Fetching audiences...")
        audiences = fetch_audiences(config)
        for audience in audiences:
            print(f"Migrating Audience: {audience['name']}")
            create_audience(config, audience)

    if migrate_config.get("databases", False):
        print("Fetching connections...")
        connections = fetch_paginated_data(config, "connections")
        sync_connections(connections, config)

if __name__ == "__main__":
    main()
