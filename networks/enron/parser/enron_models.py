import hashlib
import json
from dataclasses import dataclass, field
from itertools   import count


# ============================================================
# GLOBAL ID COUNTER
# Provides stable, sequential integer IDs for nodes.
# A single shared counter ensures uniqueness across all node
# types within one pipeline run.
# ============================================================

_node_id_counter = count(1)

def next_node_id() -> int:
    return next(_node_id_counter)


# ============================================================
# NODE MODELS
# ============================================================

@dataclass
class Person:
    """
    Represents a Person.

    Attributes:
        email  (str): normalised email address
        labels (list[str]): ["Person"]
        id     (int): auto-incremented integer, assigned on first creation
    """
    def __init__(self, email: str):
        self.labels = ["Person"]
        self.email  = email.strip().lower()
        self.id     = None          # assigned by NodesManaging on first insertion

    def get_id(self) -> int | None:
        return self.id

    def get_email(self) -> str:
        return self.email

# ============================================================
# NODES MANAGING
# ============================================================

@dataclass
class NodesManaging:
    """
    Tracks Person and Email nodes, deduplicating persons by email address.

    Person nodes are deduplicated: the same email address always maps to the
    same integer ID, even across batches (as long as the same NodesManaging
    instance is used).
    """
    def __init__(self):
        # node lists flushed to JSON at each batch boundary
        self.nodes: dict[str, list] = {
            'person': []
        }
        # deduplication registry: email_address -> int node_id
        self._person_registry: dict[str, int] = {}

    def reset_nodes(self):
        """Clear the per-batch lists without touching the deduplication registry."""
        self.nodes = {'person': []}

    # ----------------------------------------------------------
    # Person helpers
    # ----------------------------------------------------------

    def add_person(self, person: Person) -> int | None:
        """
        Adds a Person node if its email has not been seen before.

        Returns:
            The integer node_id (existing or newly assigned), or None if the
            email is empty / invalid.
        """
        email = person.get_email()
        if not email:
            return None

        if email in self._person_registry:
            return self._person_registry[email]

        person.id = next_node_id()
        self._person_registry[email] = person.id
        self.nodes['person'].append(person)
        return person.id

# ============================================================
# EDGES MANAGING
# ============================================================

@dataclass
class EdgesManaging:
    """
    Tracks direct Person -> Person edges, deduplicating by content hash.
    Each edge represents one email message reaching one recipient, and
    carries that message's timestamp directly.
 
    Edge types
    ----------
    TO   : Person -> Person   (sender -> a To recipient)
    CC   : Person -> Person   (sender -> a CC recipient)
    BCC  : Person -> Person   (sender -> a BCC recipient)
    """
    def __init__(self):
        self.edges: list[dict]  = []
        self._seen: set[str]    = set()

    def reset_edges(self):
        """Clear the per-batch edge list without touching the seen-hash set."""
        self.edges = []

    def _add(self, src_id: int, dst_id: int, edge_type: str, timestamp: str, extra: dict | None = None):
        """
        Internal helper — builds, deduplicates, and stores one edge.
 
        Args:
            src_id    : integer ID of the source (sender) node
            dst_id    : integer ID of the destination (recipient) node
            edge_type : one of TO / CC / BCC
            timestamp : ISO-8601 timestamp string of the email, included in
                        the hash so two distinct messages between the same
                        pair are never collapsed into one edge
            extra     : optional dict of additional static properties
        """
        if src_id is None or dst_id is None:
            return
        if src_id == dst_id:
            return  # skip self-addressed edges (e.g. sender CCs themselves)
 
        payload = {'src': src_id, 'dst': dst_id, 'type': edge_type, 'timestamp': timestamp}
        edge_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode('utf-8')
        ).hexdigest()
 
        if edge_hash in self._seen:
            return
 
        self._seen.add(edge_hash)
        edge = {
            'edge_id'  : edge_hash,
            'src'      : src_id,
            'dst'      : dst_id,
            'type'     : edge_type,
            'timestamp': timestamp,
        }
        if extra:
            edge.update(extra)
        self.edges.append(edge)

    def add_received_edge(self, sender_id: int, recipient_id: int, timestamp: str):
        """TO  : Person -> Person  (To field)"""
        self._add(sender_id, recipient_id, 'TO', timestamp)
 
    def add_cced_edge(self, sender_id: int, person_id: int, timestamp: str):
        """CC  : Person -> Person  (CC field)"""
        self._add(sender_id, person_id, 'CC', timestamp)
 
    def add_bcced_edge(self, sender_id: int, person_id: int, timestamp: str):
        """BCC : Person -> Person  (BCC field)"""
        self._add(sender_id, person_id, 'BCC', timestamp)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def nodes_edges_creation(
    sender      : str,
    recipients  : list[str],
    timestamp   : str,
    cc          : list[str],
    bcc         : list[str],
    node_manager: NodesManaging,
    edge_manager: EdgesManaging,
):
    """
    Converts one parsed email into nodes and edges.
 
    Graph produced per email
    ------------------------
    - 1 Person node for the sender  (deduplicated across all emails)
    - 1 Person node per recipient   (deduplicated)
    - N TO edges  : sender -> recipient, timestamped  (for each To address)
    - N CC edges  : sender -> person,    timestamped  (for each CC address)
    - N BCC edges : sender -> person,    timestamped  (for each BCC address)
 
    No Email node is created — the message's timestamp is carried directly
    on each edge it produces.
    """
    # 1. Create / retrieve sender node
    sender_id = node_manager.add_person(Person(sender))
    if sender_id is None:
        return
 
    # 2. A timestamp is required directly
    if not timestamp:
        return
 
    # 3. RECEIVED edges (TO): sender -> each To recipient
    for recipient in recipients:
        r_id = node_manager.add_person(Person(recipient))
        if r_id is not None:
            edge_manager.add_received_edge(sender_id, r_id, timestamp)
 
    # 4. CCED edges (CC)
    for person in cc:
        p_id = node_manager.add_person(Person(person))
        if p_id is not None:
            edge_manager.add_cced_edge(sender_id, p_id, timestamp)
 
    # 5. BCCED edges (BCC)
    for person in bcc:
        p_id = node_manager.add_person(Person(person))
        if p_id is not None:
            edge_manager.add_bcced_edge(sender_id, p_id, timestamp)
 