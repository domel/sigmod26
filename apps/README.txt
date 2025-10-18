README.txt
===========

Project: Graph Data Utilities — Generation, Profiling, and Validation
Date: 2025-10-18
Python: 3.9+ recommended

OVERVIEW
--------
This directory contains four complementary tools for working with graph-structured data:

1) CIDOC-CRM-gen.py
   A generator of synthetic RDF data conformant with the CIDOC-CRM ontology. It uses the
   Faker library to produce realistic labels and serializes the result as Turtle (.ttl).

2) neo_pg_stats.py
   A lightweight, streaming statistics counter for Neo4j-style CSV relationship files
   with optional filtering for a subset of relation types.

3) rdf_stats.py
   A lightweight, streaming statistics counter for very large RDF datasets in N-Triples.
   Suited to out-of-core processing and compressed inputs.

4) validation.cypher
   Cypher snippets that validate per-type binary-relation constraints (BRCs) recorded in
   a relationship property r.brc. The snippets search for counterexamples (e.g., missing
   reverse edges, self-loops, cycles, or missing a→c links) and summarize each property.

FILES AT A GLANCE
-----------------
- CIDOC-CRM-gen.py
  Dependencies: faker, rdflib
  Key imports: Faker, rdflib.Graph/Namespace/RDF/RDFS/XSD/URIRef/Literal
  Output: Turtle (.ttl) graph conformant with CIDOC-CRM

- neo_pg_stats.py
  Dependencies: standard library only
  Key imports: csv, gzip/bz2/lzma, heapq, Counter/defaultdict
  Input: Neo4j-style relationships CSV
  Feature: --interesting 'relA,relB,...' to focus analysis

- rdf_stats.py
  Dependencies: standard library only
  Key imports: csv, gzip/bz2/lzma, heapq, Counter/defaultdict
  Input: N-Triples (.nt) files; supports very large and compressed inputs

- validation.cypher
  Runtime: Neo4j 4.x or 5.x (works unchanged on both)
  Input convention: relationship property r.brc contains a set/list of BRC codes:
    REF (reflexive), SYM (symmetric), TRA (transitive), IRF (irreflexive),
    ASM (asymmetric), ANS (antisymmetric), ACC (acyclic), ITR (intransitive),
    ANT (anti-transitive)

SYSTEM REQUIREMENTS
-------------------
- Python 3.9 or newer
- For CIDOC-CRM-gen.py:
    pip install faker rdflib
- For the CSV/RDF statistics scripts:
    standard Python libraries (no extra packages required)
- For validation.cypher:
    Neo4j Desktop or a Neo4j Server (4.x or 5.x) and access via Browser or cypher-shell

INSTALLATION (OPTIONAL VENV)
----------------------------
  python3 -m venv .venv
  . .venv/bin/activate            # Windows: .venv\Scripts\activate
  pip install --upgrade pip
  pip install faker rdflib

DATA FORMATS
------------
A) Neo4j-style relationships CSV (for neo_pg_stats.py):
   Required columns (typical):
     node_id_start, node_id_end, rel_type
   Notes:
   - Additional columns are ignored unless the script documents otherwise.
   - Compressed inputs (*.csv.gz, *.csv.bz2, *.csv.xz) are supported by extension.

B) RDF N-Triples (for rdf_stats.py):
   - Plain .nt or compressed .nt.gz/.nt.bz2/.nt.xz
   - Each line: <subject> <predicate> <object> .

C) Property-graph validation model (for validation.cypher):
   - Relationship property r.brc is a collection (list/array) of strings drawn from:
       REF, SYM, TRA, IRF, ASM, ANS, ACC, ITR, ANT
   - Examples:
       ['REF','SYM','TRA']          // an equivalence-like relation
       ['IRF','ACC','ASM']          // strict partial order characteristics

QUICK START
-----------
1) Generate a synthetic CIDOC-CRM dataset:
   - Show help:
       python CIDOC-CRM-gen.py -h
   - Produce a Turtle graph (examples; adapt to the actual CLI shown by -h):
       python CIDOC-CRM-gen.py > out.ttl
     TIP: Many generators accept options like --seed, --count, or --output.
          Use -h to discover the exact interface.

2) Compute statistics on a Neo4j relationships CSV:
   - Show help:
       python neo_pg_stats.py -h
   - Example (focus on a subset of relations):
       python neo_pg_stats.py --interesting 'registered_address,intermediary_of' \
         --edges relationships.csv.gz > rel_stats.csv
     Notes:
       • The script is streaming and can handle large compressed inputs.
       • Typical outputs include counts, degree summaries, and simple top-K profiles.

3) Compute RDF statistics for a large N-Triples file:
   - Show help:
       python rdf_stats.py -h
   - Example (compressed input):
       python rdf_stats.py --input dump.nt.gz > rdf_stats.csv
     Notes:
       • Designed for very large files; uses heap-based and streaming aggregations.
       • Consult -h for available reports (predicate distribution, co-occurrence, etc.).

4) Validate binary-relation constraints in Neo4j:
   - Load your data and ensure r.brc is present on relevant relationships.
   - Open Neo4j Browser and paste the contents of validation.cypher, or run via:
       cypher-shell -u neo4j -p '***' -f validation.cypher
   - Each block searches for counterexamples and returns a boolean summary per rel type.

USAGE NOTES AND BEST PRACTICES
------------------------------
- Reproducibility (generation):
  If the generator exposes a --seed argument, fix it (e.g., --seed 42) to obtain
  repeatable synthetic graphs.

- Large files (profilers):
  The statistics scripts are streaming and support gzip/bz2/xz by file extension.
  Prefer gzip for speed and xz for maximum compression when storage is limited.

- Column hygiene (CSV):
  Ensure the header row matches expected column names exactly:
    node_id_start,node_id_end,rel_type
  Pre-clean malformed rows before profiling to avoid skew in counts.

- BRC semantics (validation):
  The snippets treat each constraint independently and look for explicit
  counterexamples. For example:
    SYM  → for each a—R→b marked SYM, there should exist b—R→a
    TRA  → for a—R→b and b—R→c marked TRA, there should exist a—R→c
    IRF  → no a—R→a edges
    ACC  → no directed cycles for R
  Use them as building blocks in property-graph validation pipelines.

TROUBLESHOOTING
---------------
- “ModuleNotFoundError: No module named 'faker' or 'rdflib'”
  → Install dependencies: pip install faker rdflib

- “UnicodeDecodeError” on CSV/RDF inputs
  → Re-export as UTF-8; pass through iconv or recode if needed.

- Slow runs on multi-GB inputs
  → Prefer .gz over .bz2/.xz for speed; run on SSD; avoid piping through tools
    that buffer entire files in memory.

- Neo4j validation returns false positives
  → Confirm that r.brc is set only where the constraint is intended to hold and that
    the relation type grouping in the query matches your naming conventions.

SECURITY & PRIVACY
------------------
- The generator uses Faker to produce synthetic labels; nevertheless, verify that no
  real personal data is included in your seed lists or templates.
- When profiling proprietary datasets, avoid committing raw inputs or derived
  statistics that may leak sensitive metadata.

ACKNOWLEDGEMENTS
----------------
- CIDOC-CRM: https://www.cidoc-crm.org/
- rdflib: https://rdflib.readthedocs.io/
- Faker: https://faker.readthedocs.io/
- Neo4j Cypher: https://neo4j.com/developer/cypher/



