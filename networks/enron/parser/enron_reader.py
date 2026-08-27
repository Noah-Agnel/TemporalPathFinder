import os
import json
from pathlib         import Path
from email           import message_from_file
from email.utils     import parsedate_to_datetime
from tqdm            import tqdm
from enron_models    import NodesManaging, EdgesManaging, nodes_edges_creation
from enron_json      import static_nodes_json_creation, static_edges_json_creation,node_filename, edge_filename

# ============================================================
# CONFIGURATION
# ============================================================
config       = json.load(open("../config/config.json"))
maildir_path = config["maildir_path"]
output_path  = config.get("output_path", "../../full_temporal_mri_import")
batch_size   = config.get("batch_size", 50000)


# ============================================================
# EMAIL PARSING
# ============================================================

def parse_email_file(filepath):
    """
    Parses a single email file and extracts:
    - sender
    - recipients (To)
    - cc
    - bcc
    - timestamp
    """
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            msg = message_from_file(f)

        sender = msg.get('From', None)
        if sender is None:
            return None

        to_raw  = msg.get('To',    '')
        cc_raw  = msg.get('X-cc',  '')
        bcc_raw = msg.get('X-bcc', '')

        recipients = [r.strip().lower() for r in to_raw.split(',')  if r.strip()]
        cc         = [r.strip().lower() for r in cc_raw.split(',')  if r.strip()]
        bcc        = [r.strip().lower() for r in bcc_raw.split(',') if r.strip()]

        if not recipients:
            return None

        date_raw = msg.get('Date', None)
        if date_raw is None:
            return None

        try:
            timestamp = parsedate_to_datetime(date_raw).isoformat()
        except Exception:
            return None

        return {
            'sender'    : sender.strip().lower(),
            'recipients': recipients,
            'cc'        : cc,
            'bcc'       : bcc,
            'timestamp' : timestamp
        }

    except Exception:
        return None


def collect_all_email_files(maildir):
    """
    Walks the entire maildir and collects all email file paths.
    Skips non-email files.
    """
    all_files = []
    for root, dirs, files in os.walk(maildir):
        for fname in files:
            if fname.endswith('.') or fname[-1].isdigit():
                all_files.append(os.path.join(root, fname))
    return all_files


# ============================================================
# BATCH SAVING
# ============================================================

def save_batch(node_manager, edge_manager, output_path, batch_num):
    """
    Serialises the current batch of nodes and edges to JSON files.

    One file is written for the person node type (the only node type now
    that Email is no longer represented as a node) and one for all edges.
    Empty node lists are skipped — the Spark pipeline would read an empty
    array fine, but there is no point creating the file.

    Args:
        node_manager (NodesManaging): holds the current batch's node lists
        edge_manager (EdgesManaging): holds the current batch's edge list
        output_path  (str): directory to write files into
        batch_num    (int): batch index used in the filename
    """
    nodes_json = static_nodes_json_creation(node_manager.nodes)
    edges_json = static_edges_json_creation(edge_manager.edges)

    for node_type, records in nodes_json.items():
        if not records:
            continue
        path = node_filename(node_type, output_path, batch_num)
        with open(path, "w") as f:
            json.dump(records, f, indent=4)
        print(f"  Wrote {len(records):>6} {node_type} nodes -> {path}")

    path = edge_filename(output_path, batch_num)
    with open(path, "w") as f:
        json.dump(edges_json, f, indent=4)
    print(f"  Wrote {len(edges_json):>6} edges          -> {path}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("Collecting email files...")
    all_files = collect_all_email_files(maildir_path)
    print(f"Found {len(all_files)} email files")

    # limit for testing on low-memory machines — remove or raise for full runs
    #all_files = all_files[:10000]
    print(f"Processing {len(all_files)} files")

    Path(output_path).mkdir(parents=True, exist_ok=True)

    node_manager  = NodesManaging()
    edge_manager  = EdgesManaging()
    batch_num     = 0
    emails_in_batch = 0

    for filepath in tqdm(all_files):
        parsed = parse_email_file(filepath)
        if parsed is None:
            continue

        nodes_edges_creation(
            parsed['sender'],
            parsed['recipients'],
            parsed['timestamp'],
            parsed['cc'],
            parsed['bcc'],
            node_manager,
            edge_manager,
        )

        emails_in_batch += 1

        if emails_in_batch >= batch_size:
            print(f"\nSaving batch {batch_num}...")
            save_batch(node_manager, edge_manager, output_path, batch_num)
            node_manager.reset_nodes()
            edge_manager.reset_edges()
            emails_in_batch = 0
            batch_num      += 1

    # flush the final (possibly partial) batch
    if edge_manager.edges:
        print(f"\nSaving final batch {batch_num}...")
        save_batch(node_manager, edge_manager, output_path, batch_num)

    print("\nDone!")