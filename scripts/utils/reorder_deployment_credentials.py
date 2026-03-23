import json
from pathlib import Path


def reorder_dict(original: dict, key_order: list[str]) -> dict:
    """
    Return a new dict with keys reordered according to key_order, then any
    remaining keys appended in their original order. Values are unchanged.
    """
    result: dict = {}

    # First, add keys in the preferred order if they exist
    for key in key_order:
        if key in original:
            result[key] = original[key]

    # Then append any remaining keys in their original order
    for key in original.keys():
        if key not in result:
            result[key] = original[key]

    return result


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent.parent
    src = repo_root / "deployment_credentials.json"
    dst = repo_root / "deployment_credentials_ordered.json"

    print(f"Reordering deployment credentials", flush=True)
    print(f"  Source: {src}", flush=True)
    print(f"  Dest:   {dst}", flush=True)

    with src.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Top-level ordering: lifecycle-aligned and grouped
    top_level_order = [
        "metadata",
        "aws_account",
        "environments",
        "demo_frontend",
        "demo_database",
        "backend_api",
        "clinic_frontend",
        "clinic_database",
        "commands",
    ]
    if isinstance(data, dict):
        data = reorder_dict(data, top_level_order)

    # Reorder environments: local -> test -> demo -> clinic
    envs = data.get("environments")
    if isinstance(envs, dict):
        env_order = ["local", "test", "demo", "clinic"]
        data["environments"] = reorder_dict(envs, env_order)

    # Reorder backend_api keys to show demo cluster first, then clinic cluster
    backend_api = data.get("backend_api")
    if isinstance(backend_api, dict):
        backend_order = [
            # Demo cluster
            "environment",
            "note",
            "api_url",
            "domain",
            "purpose",
            "database",
            "api_environment_file",
            "previous_name",
            "api_key",
            "network",
            "application_load_balancer",
            "ssl_certificate",
            "ec2",
            "database_connection",
            # Clinic cluster
            "clinic_api",
            "clinic_database_reference",
            "route53",
        ]
        data["backend_api"] = reorder_dict(backend_api, backend_order)

    # Reorder commands: demo-focused first, then clinic-specific
    commands = data.get("commands")
    if isinstance(commands, dict):
        commands_order = [
            "access_api_ec2",
            "check_api_instance_status",
            "access_demo_db_ec2",
            "check_demo_db_instance_status",
            "check_target_group_health",
            "check_alb_status",
            "invalidate_cloudfront",
            "invalidate_clinic_cloudfront",
        ]
        data["commands"] = reorder_dict(commands, commands_order)

    with dst.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
        f.write("\n")


if __name__ == "__main__":
    main()


