"""
read_yaml — reads YAML files (schema contracts, QC contracts, configs).

Used by the agent to load Bouncer contracts from /schemas/ in the
container, or to inspect user-supplied YAML config files.
"""


def read_yaml(path: str) -> dict:
    """
    Read a YAML file and return its parsed contents.

    Args:
        path: Absolute path to the YAML file.

    Returns dict with:
        path, content (parsed YAML as dict/list)
    """
    try:
        import yaml
    except ImportError:
        return {"path": path, "error": "pyyaml not installed"}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)

        return {
            "path": path,
            "content": content,
            "top_keys": list(content.keys()) if isinstance(content, dict) else None,
        }

    except Exception as e:
        return {"path": path, "error": str(e)}
