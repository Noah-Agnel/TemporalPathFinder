# ============================================================
# STATIC NODES JSON CREATION
# ============================================================

# Mapping from Python node type key -> Spark filename segment.
# The Spark pipeline derives the inner DataFrame key (mapKey) by
# slicing filename parts [2..-1], so a file named
#   path_1_<FILE_SEGMENT>_<batch>.json
# produces mapKey = <FILE_SEGMENT>.
#
# Node files are routed into nodesDF  via the "node" substring check.
# Edge files are routed into edgesDF  via the absence of "node".
#
# "email" is no longer a node type — every email message is now
# represented as timestamped edges between Person nodes instead.

_NODE_FILE_SEGMENT = {
    "person": "person_nodes_static_props",
}

# Single segment for the edges file — all edge types share one file per batch.
_EDGE_FILE_SEGMENT = "edges_static_props"


def _node_to_dict(node) -> dict:
    """
    Converts any node object into the JSON record expected by the Spark pipeline.

    Expected Spark schema per record
    ---------------------------------
    {
        "node_id"     : int,          # stable integer ID
        "labels"      : [str, ...],   # e.g. ["Person"]
        "static_props": { ... },      # all remaining fields (type-specific)
        "is_active"   : bool
    }

    The function pops `id` and `labels` from the node's __dict__ copy and
    treats every remaining attribute as a static property.
    """
    node_dict = node.__dict__.copy()
    node_id   = node_dict.pop("id")
    labels    = node_dict.pop("labels")
    return {
        "node_id"     : node_id,
        "labels"      : labels,
        "static_props": node_dict,   # Person → {"email": ...}
        "is_active"   : True
    }


def static_nodes_json_creation(nodes: dict) -> dict:
    """
    Converts node objects into the JSON format expected by the Spark pipeline.

    Args:
        nodes (dict): dictionary of node lists keyed by node type
                      e.g. {"person": [...]}

    Returns:
        dict: {node_type: [record, ...]} ready for JSON serialisation.
              The dict keys are the same node_type strings; callers use
              _NODE_FILE_SEGMENT to map them to the correct output filename.
    """
    return {
        node_type: [_node_to_dict(node) for node in node_list]
        for node_type, node_list in nodes.items()
    }


# ============================================================
# STATIC EDGES JSON CREATION
# ============================================================

def _edge_to_dict(edge: dict) -> dict:
    """
    Converts one internal edge dict into the JSON record expected by Spark.

    Internal edge schema (from EdgesManaging)
    ------------------------------------------
    {
        "edge_id"  : str (SHA-256 hex),
        "src"      : int,
        "dst"      : int,
        "type"     : str,   # TO | CC | BCC
        "timestamp": str,   # ISO-8601 timestamp of the email
    }

    Output schema
    -------------
    {
        "edge_id"     : str,
        "source_id"   : int,
        "target_id"   : int,
        "edge_type"   : str,
        "static_props": { "timestamp": str }
    }

    static_props now carries the email's timestamp: with no Email node to
    hold it, the send time has to live directly on the edge. It lands in
    the edge_static_props Iceberg table as a datetime_value row keyed by
    property_name="timestamp" — no schema change required on that side.
    """
    edge = edge.copy()
    return {
        "edge_id"     : edge["edge_id"],
        "source_id"   : edge["src"],
        "target_id"   : edge["dst"],
        "edge_type"   : edge["type"],
        "static_props": {"timestamp": edge["timestamp"]}
    }


def static_edges_json_creation(edges: list) -> list:
    """
    Converts edge dicts into the JSON format expected by the Spark pipeline.

    Args:
        edges (list): list of edge dicts produced by EdgesManaging

    Returns:
        list: list of edge JSON records, one per edge.
    """
    return [_edge_to_dict(edge) for edge in edges]

# ============================================================
# FILENAME HELPERS
# ============================================================

def node_filename(node_type: str, output_path: str, batch_num: int) -> str:
    """
    Returns the output path for a node JSON file.
    e.g. path_1_person_static_props_0.json
    """
    import os
    return os.path.join(output_path, f"path_1_{node_type}_static_props_{batch_num}.json")


def edge_filename(output_path: str, batch_num: int) -> str:
    """
    Returns the output path for an edge JSON file.
    e.g. path_1_edges_static_props_0.json
    """
    import os
    return os.path.join(output_path, f"path_1_edges_static_props_{batch_num}.json")