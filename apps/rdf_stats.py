#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rdf_stats.py — Lightweight, streaming RDF statistics counter for very large N-Triples files.

What’s new (version 1.3):
- FIXED: files 8–13 could be empty / zero when `--predicates-file` contained
  IRIs with HTTPS instead of HTTP (e.g., Wikidata uses HTTP in dumps), or CURIEs (e.g., rdf:type).
  Introduced canonicalization (ANGLE IRI) and HTTP↔HTTPS aliasing.
- Interesting predicates are guaranteed to be included in Top-K co-occurrences
  regardless of format (HTTP/HTTPS/CURIE) — we build a set of *data tokens* for the interesting ones.
- Aggregation of statistics 8–13 over *aliases* (e.g., HTTP and HTTPS counted jointly into one row,
  presented as in the input `--predicates-file`).
- (Optional) `--prefixes <file.json>` — prefix mapping for expanding CURIEs
  (rdf, rdfs, xsd, owl, skos, schema, foaf, dc, dcterms, wikidata — built-in by default).
- Diagnostics on STDERR: how many of the interesting predicates actually occurred (after aliasing) and which did not.
- (NEW) VISUALIZATIONS: `--plots` (PNGs in outdir):
    * histogram of predicate frequency distribution,
    * bar chart of Top-N predicates,
    * co-occurrence heatmap (Top-N),
    * (optional) co-occurrence heatmap of interesting predicates (after aliasing).
    * Parameters: `--plots-topN`, `--plots-dpi`, `--plots-curie-labels`.

Outputs (CSV):
  1_predicate_distribution.csv           — p, count, share
  1_meta_entropy_gini.csv                — N_triples, N_predicates, entropy_bits, gini
  2_per_predicate_basic.csv              — p, unique_subjects, unique_objects, avg_* etc.
  3_predicate_cooccurrence.csv           — co-occurrences (global)
  4_association_rules.csv                — association rules (global)
  5_per_predicate_graph_metrics.csv      — centralities (if networkx is available)
  6_unique_so_per_predicate.csv          — (optional --unique-so)
  7_duplicate_excess_per_predicate.csv   — (optional --duplicate-excess)
  8_interesting_predicates_summary.csv   — SUMMARY for the list of interesting predicates
  9_interesting_predicates_datatypes.csv — datatype distribution of literals
 10_interesting_predicates_langs.csv     — language distribution of literals
 11_interesting_predicates_top_objects.csv — Top-N object IRIs
 12_interesting_predicates_cooccurrence.csv — co-occurrences involving interesting predicates (after aliasing)
 13_interesting_predicates_rules.csv       — rules A->B with an interesting antecedent (after aliasing)

Author: (Your name)
Version: 1.3
"""

import argparse
import bz2
import csv
import gzip
import heapq
import io
import itertools
import json
import lzma
import math
import os
import re
import sys
import tempfile
from collections import Counter, defaultdict
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Set

# ----------------------
# N-Triples parsing
# ----------------------

# Regex for the first two tokens (S, P) and the rest (O) up to the dot.
NT_PATTERN = re.compile(r'^\s*(<[^>]*>|_:[^\s]+)\s+(<[^>]*>)\s+(.*)\s*\.\s*$')
TYPED_RE = re.compile(r'\^\^<([^>]+)>\s*$')  # datatype
LANG_RE  = re.compile(r'\"(?:[^\"\\]|\\.)*\"@([a-zA-Z0-9\-]+)\s*$')  # @lang
XSD_STRING = '<http://www.w3.org/2001/XMLSchema#string>'

def parse_nt_line(line: str) -> Optional[Tuple[str, str, str]]:
    m = NT_PATTERN.match(line)
    if not m:
        return None
    s, p, o = m.group(1), m.group(2), m.group(3)
    return s, p, o


# ----------------------
# Prefixes and IRI canonicalization/aliases
# ----------------------
DEFAULT_PREFIXES: Dict[str, str] = {
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    'xsd': 'http://www.w3.org/2001/XMLSchema#',
    'owl': 'http://www.w3.org/2002/07/owl#',
    'skos': 'http://www.w3.org/2004/02/skos/core#',
    'schema': 'http://schema.org/',
    'foaf': 'http://xmlns.com/foaf/0.1/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/',
    'wikidata': 'http://www.wikidata.org/prop/direct/'
}
CURIE_RE = re.compile(r'^([A-Za-z_][\w\-]*)\:([^\s:][^\s]*)$')  # prefix:local (no ://)


def canon_angle(qname_or_iri: str, prefixes: Dict[str, str]) -> Optional[str]:
    """Returns an IRI in angle brackets <…>.
    Supports: <…>, full IRIs without brackets, CURIE (e.g., rdf:type).
    """
    t = qname_or_iri.strip()
    if not t:
        return None
    if t.startswith('<') and t.endswith('>'):
        return t
    if '://' in t:
        return f'<{t}>'
    m = CURIE_RE.match(t)
    if m:
        pref, local = m.group(1), m.group(2)
        base = prefixes.get(pref)
        if base:
            return f'<{base}{local}>'
    return None


def http_https_variants(angle_iri: str) -> Set[str]:
    """Returns the set of HTTP/HTTPS variants for a given <…> (at least the angle_iri itself)."""
    out = {angle_iri}
    if not (angle_iri.startswith('<http://') or angle_iri.startswith('<https://')):
        return out
    inside = angle_iri[1:-1]
    if inside.startswith('http://'):
        out.add(f'<https://{inside[len("http://"):]}>' )
    elif inside.startswith('https://'):
        out.add(f'<http://{inside[len("https://"):]}>' )
    return out


def build_interesting_aliases(interesting_display: Set[str]) -> Tuple[Dict[str, str], Dict[str, Set[str]]]:
    """Build alias maps: data_token -> display_token and display_token -> {data_tokens}."""
    rev: Dict[str, str] = {}
    disp2data: Dict[str, Set[str]] = {}
    for disp in interesting_display:
        variants = http_https_variants(disp)
        disp2data.setdefault(disp, set()).update(variants)
        for v in variants:
            rev[v] = disp
    return rev, disp2data

# ----------------------
# External sorting
# ----------------------

def open_maybe_compressed(path: str) -> io.TextIOBase:
    lower = path.lower()
    if lower.endswith('.gz'):
        return io.TextIOWrapper(gzip.open(path, 'rb'), encoding='utf-8', errors='replace')
    if lower.endswith('.bz2'):
        return io.TextIOWrapper(bz2.open(path, 'rb'), encoding='utf-8', errors='replace')
    if lower.endswith('.xz') or lower.endswith('.lzma'):
        return io.TextIOWrapper(lzma.open(path, 'rb'), encoding='utf-8', errors='replace')
    return open(path, 'r', encoding='utf-8', errors='replace')


def chunked_sorted_files(raw_path: str, chunk_lines: int, key_is_prefix: bool=True) -> List[str]:
    chunk_paths = []
    with open(raw_path, 'r', encoding='utf-8', errors='replace') as f:
        buf = []
        for i, line in enumerate(f, 1):
            buf.append(line)
            if len(buf) >= chunk_lines:
                buf.sort()
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.chunk', mode='w', encoding='utf-8')
                tmp.writelines(buf)
                tmp.close()
                chunk_paths.append(tmp.name)
                buf.clear()
        if buf:
            buf.sort()
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.chunk', mode='w', encoding='utf-8')
            tmp.writelines(buf)
            tmp.close()
            chunk_paths.append(tmp.name)
    return chunk_paths


def merge_sorted_chunks(chunk_paths: List[str]) -> Iterator[str]:
    files = [open(p, 'r', encoding='utf-8', errors='replace') for p in chunk_paths]
    try:
        for line in heapq.merge(*files):
            yield line
    finally:
        for fh in files:
            fh.close()

# ----------------------
# Auxiliary metrics
# ----------------------

def shannon_entropy(probs: Iterable[float]) -> float:
    return -sum(p * math.log2(p) for p in probs if p > 0)


def gini_index(probs: Iterable[float]) -> float:
    return 1.0 - sum(p*p for p in probs)

# ----------------------
# VISUALIZATIONS (optional)
# ----------------------
try:
    import matplotlib
    matplotlib.use("Agg")  # headless backend
    import matplotlib.pyplot as plt
except Exception:
    matplotlib = None  # no matplotlib -> skip plotting

try:
    import numpy as np  # optional for heatmaps
except Exception:
    np = None


def _make_prefix_index(prefixes: Dict[str, str]) -> List[Tuple[str, str]]:
    """Return a list (prefix, baseIRI) sorted descending by baseIRI length (longest matches first)."""
    return sorted(prefixes.items(), key=lambda kv: len(kv[1]), reverse=True)

def iri_to_curie(angle_iri: str, prefix_index: List[Tuple[str, str]]) -> Optional[str]:
    """Attempts to map <IRI> to CURIE (prefix:local) using known prefixes."""
    if not (angle_iri.startswith('<') and angle_iri.endswith('>')):
        return None
    inside = angle_iri[1:-1]
    for pref, base in prefix_index:
        if inside.startswith(base):
            return f"{pref}:{inside[len(base):]}"
    return None

def friendly_label(token: str, prefixes: Dict[str, str], prefer_curie: bool=True) -> str:
    """Friendly axis label: CURIE (if possible), else fragment after # or last segment after /, else raw token."""
    t = token.strip()
    if not t:
        return t
    # if it is already a CURIE (prefix:local)
    if CURIE_RE.match(t):
        return t
    # if <IRI>, try CURIE
    if t.startswith('<') and t.endswith('>'):
        if prefer_curie:
            curie = iri_to_curie(t, _make_prefix_index(prefixes))
            if curie:
                return curie
        inside = t[1:-1]
        if '#' in inside:
            return inside.split('#')[-1]
        if '/' in inside:
            return inside.rstrip('/').split('/')[-1]
        return inside
    return t

def plot_hist_counts(pred_counts: Dict[str, int], outdir: str, dpi: int):
    if matplotlib is None:
        return
    counts = list(pred_counts.values())
    if not counts:
        return
    plt.figure(figsize=(8, 5))
    plt.hist(counts, bins='auto')
    plt.title('Histogram of predicate counts')
    plt.xlabel('Predicate occurrence count')
    plt.ylabel('Number of predicates')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, 'plot_1a_predicate_count_histogram.png'), dpi=dpi)
    plt.close()

def plot_bar_top_predicates(pred_counts: Dict[str, int], outdir: str, topN: int, dpi: int, prefixes: Dict[str, str], prefer_curie: bool):
    if matplotlib is None:
        return
    items = sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True)[:max(1, topN)]
    if not items:
        return
    labels = [friendly_label(p, prefixes, prefer_curie) for p, _ in items]
    values = [c for _, c in items]
    width = max(9.0, 0.45 * len(labels))
    plt.figure(figsize=(width, 6))
    plt.bar(range(len(values)), values)
    plt.xticks(range(len(labels)), labels, rotation=80, ha='right')
    plt.title(f'Top-{len(labels)} predicates (count)')
    plt.xlabel('Predicate')
    plt.ylabel('Count')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, f'plot_1b_top_predicates_bar_top{len(labels)}.png'), dpi=dpi)
    plt.close()

def plot_heatmap_cooccurrence(preds: List[str],
                              pair_counts: Dict[Tuple[str, str], int],
                              pred_support: Dict[str, int],
                              outdir: str, dpi: int,
                              prefixes: Dict[str, str], prefer_curie: bool,
                              filename: str):
    if matplotlib is None or not preds:
        return
    k = len(preds)
    # indices
    idx = {p: i for i, p in enumerate(preds)}
    # symmetric matrix with n_ab; diagonal holds support n_a
    M = [[0] * k for _ in range(k)]
    for (a, b), n_ab in pair_counts.items():
        if a in idx and b in idx:
            i, j = idx[a], idx[b]
            if i == j:
                continue
            M[i][j] += n_ab
            M[j][i] += n_ab
    for p, i in idx.items():
        M[i][i] = pred_support.get(p, 0)
    if np is not None:
        data = np.array(M, dtype=float)
    else:
        data = M
    # figure size dynamically adjusted to the number of labels
    side = max(6.0, 0.40 * k)
    plt.figure(figsize=(side, side))
    im = plt.imshow(data, aspect='auto', interpolation='nearest')
    plt.colorbar(im, fraction=0.046, pad=0.04)
    tick_labels = [friendly_label(p, prefixes, prefer_curie) for p in preds]
    plt.xticks(range(k), tick_labels, rotation=80, ha='right')
    plt.yticks(range(k), tick_labels)
    plt.title('Predicate co-occurrence (n_ab; diag: n_a)')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, filename), dpi=dpi)
    plt.close()

# ----------------------
# Main logic
# ----------------------

def main():
    ap = argparse.ArgumentParser(description='Lightweight RDF statistics counter for very large N-Triples files.')
    ap.add_argument('--input', required=True, help='Path to .nt file (.gz/.bz2/.xz allowed).')
    ap.add_argument('--outdir', required=True, help='Output directory for CSV files.')
    ap.add_argument('--tmpdir', default=None, help='Temporary directory (system default if omitted).')
    ap.add_argument('--chunk-lines', type=int, default=500_000, help='Chunk size for external sort (number of lines).')
    ap.add_argument('--topk', type=int, default=200, help='Top-K predicates for co-occurrences.')
    ap.add_argument('--max-preds-per-subject', type=int, default=50, help='Limit of predicates per subject (for co-occurrences).')
    ap.add_argument('--min-support', type=float, default=0.01, help='Minimum rule support (e.g., 0.01).')
    ap.add_argument('--min-conf', type=float, default=0.5, help='Minimum rule confidence (e.g., 0.5).')
    ap.add_argument('--unique-so', action='store_true', help='Count the number of unique (s,o) pairs per predicate (slower).')
    ap.add_argument('--duplicate-excess', action='store_true', help='Count redundant duplicates of (s,o) per predicate (slower).')
    # NEW
    ap.add_argument('--predicates-file', default=None, help='File with a list of interesting predicates (one IRI/CURIE per line).')
    ap.add_argument('--top-values', type=int, default=50, help='Top-N most frequent object IRIs for interesting predicates.')
    ap.add_argument('--prefixes', default=None, help='JSON with a prefix map to expand CURIEs (e.g., {"rdf":"http://…#"}).')

    # --- plots ---
    ap.add_argument('--plots', action='store_true',
                    help='Generate PNG plots in the output directory.')
    ap.add_argument('--plots-topN', type=int, default=30,
                    help='Number of predicates in Top-N plots (default 30).')
    ap.add_argument('--plots-dpi', type=int, default=150,
                    help='DPI for PNG outputs (default 150).')
    ap.add_argument('--plots-curie-labels', action='store_true',
                    help='Use CURIE labels on axes when possible.')

    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    tmp_ctx = tempfile.TemporaryDirectory(dir=args.tmpdir)
    tmpdir = tmp_ctx.name

    # Prefix maps
    prefixes = DEFAULT_PREFIXES.copy()
    if args.prefixes:
        try:
            with open(args.prefixes, 'r', encoding='utf-8') as pf:
                user_pref = json.load(pf)
                if isinstance(user_pref, dict):
                    prefixes.update({str(k): str(v) for k, v in user_pref.items()})
                else:
                    print('[WARN] The --prefixes file does not contain a JSON object (prefix map). Ignoring.', file=sys.stderr)
        except Exception as e:
            print(f'[WARN] Failed to load --prefixes: {e}', file=sys.stderr)

    # List of interesting predicates (optional) — *display* in the form <…>
    interesting_display: Set[str] = set()
    if args.predicates_file:
        with open(args.predicates_file, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                if not line.strip() or line.lstrip().startswith('#'):
                    continue
                ang = canon_angle(line, prefixes)
                if ang:
                    interesting_display.add(ang)
                else:
                    print(f"[WARN] Unrecognized entry in --predicates-file: {line.rstrip()}", file=sys.stderr)

    # Aliasing: data_token -> display_token and display_token -> {data_tokens}
    alias_rev: Dict[str, str] = {}
    disp2data: Dict[str, Set[str]] = {}
    if interesting_display:
        alias_rev, disp2data = build_interesting_aliases(interesting_display)

    # RAW files
    ps_raw = os.path.join(tmpdir, 'ps.raw')
    po_raw = os.path.join(tmpdir, 'po.raw')
    sp_raw = os.path.join(tmpdir, 'sp.raw')
    need_pso = bool(args.unique_so or args.duplicate_excess or interesting_display)
    pso_raw = os.path.join(tmpdir, 'pso.raw') if need_pso else None

    # Pass 1: streaming over the input
    pred_counts: Dict[str, int] = Counter()
    N_triples = 0
    bad_lines = 0

    # Additional structures for interesting predicates (KEYED BY *display*!)
    subj_kind_counts: Dict[str, Counter] = defaultdict(Counter)
    obj_kind_counts:  Dict[str, Counter] = defaultdict(Counter)
    datatype_counts:  Dict[str, Counter] = defaultdict(Counter)
    lang_counts:      Dict[str, Counter] = defaultdict(Counter)
    obj_top_iris:     Dict[str, Counter] = defaultdict(Counter)
    literals_total:   Dict[str, int] = Counter()

    with open(ps_raw, 'w', encoding='utf-8') as f_ps, \
         open(po_raw, 'w', encoding='utf-8') as f_po, \
         open(sp_raw, 'w', encoding='utf-8') as f_sp, \
         (open(pso_raw, 'w', encoding='utf-8') if pso_raw else nullcontext()) as f_pso, \
         open_maybe_compressed(args.input) as fin:

        for line in fin:
            line = line.rstrip('\n')
            if not line or line.startswith('#'):
                continue
            parsed = parse_nt_line(line)
            if not parsed:
                bad_lines += 1
                continue
            s, p, o = parsed
            N_triples += 1
            pred_counts[p] += 1

            # write global keys (by original p)
            f_ps.write(f"{p}\t{s}\n")
            f_po.write(f"{p}\t{o}\n")
            f_sp.write(f"{s}\t{p}\n")

            # If interesting (after alias), count under the *display* key
            disp = alias_rev.get(p)
            if disp:
                # pso for unique/dups for interesting predicates aggregated by *display*
                if pso_raw:
                    f_pso.write(f"{disp}\t{s}\t{o}\n")
                # subject/object “telemetry”
                subj_kind_counts[disp]['IRI' if s.startswith('<') else 'BNODE'] += 1
                if o.startswith('<'):
                    obj_kind_counts[disp]['IRI'] += 1
                    obj_top_iris[disp][o] += 1
                elif o.startswith('_:'):
                    obj_kind_counts[disp]['BNODE'] += 1
                else:
                    obj_kind_counts[disp]['LITERAL'] += 1
                    literals_total[disp] += 1
                    m_dt = TYPED_RE.search(o)
                    if m_dt:
                        datatype_counts[disp][f"<{m_dt.group(1)}>"] += 1
                    else:
                        m_lang = LANG_RE.search(o)
                        if m_lang:
                            lang_counts[disp][m_lang.group(1).lower()] += 1
                        else:
                            datatype_counts[disp][XSD_STRING] += 1
            else:
                # if not interesting, but global pso requested (unique/dups)
                if pso_raw and (args.unique_so or args.duplicate_excess) and not interesting_display:
                    f_pso.write(f"{p}\t{s}\t{o}\n")

    if bad_lines:
        print(f"[WARN] Skipped {bad_lines} lines not conforming to N-Triples.", file=sys.stderr)

    # Diagnostics of matches for interesting predicates after aliasing
    if interesting_display:
        matched = []
        for disp, tokens in disp2data.items():
            if any(t in pred_counts for t in tokens):
                matched.append(disp)
        print(f"[INFO] Interesting predicates: {len(interesting_display)}; matched in data (after aliasing): {len(matched)}.", file=sys.stderr)
        if len(matched) < len(interesting_display):
            missing = sorted(d for d in interesting_display if d not in set(matched))
            print('[HINT] No match for:', file=sys.stderr)
            for d in missing[:10]:
                print(f'       {d}', file=sys.stderr)

    # Predicate distribution (global)
    N_predicates = len(pred_counts)
    shares = [c / N_triples for c in pred_counts.values()] if N_triples else []
    entropy = shannon_entropy(shares) if shares else 0.0
    gini = gini_index(shares) if shares else 0.0

    # Write 1_* CSV
    with open(os.path.join(args.outdir, '1_predicate_distribution.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow(['predicate', 'count', 'share'])
        for p, c in sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True):
            w.writerow([p, c, (c / N_triples if N_triples else 0.0)])

    with open(os.path.join(args.outdir, '1_meta_entropy_gini.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow(['N_triples', 'N_predicates', 'entropy_bits', 'gini'])
        w.writerow([N_triples, N_predicates, entropy, gini])

    # Determine Top-K predicates for co-occurrences — by data tokens; include interesting tokens
    topk_n = args.topk if args.topk and args.topk > 0 else 0
    top_pred_tokens: Set[str] = set()
    if topk_n:
        top_pred_tokens = {p for p, _ in sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True)[:topk_n]}
    if interesting_display:
        for tokens in disp2data.values():
            top_pred_tokens |= tokens

    # External sorting and computations
    ps_chunks = chunked_sorted_files(ps_raw, args.chunk_lines)
    po_chunks = chunked_sorted_files(po_raw, args.chunk_lines)
    sp_chunks = chunked_sorted_files(sp_raw, args.chunk_lines)
    pso_chunks = chunked_sorted_files(pso_raw, args.chunk_lines) if pso_raw else []

    # --- (A) ps: unique_subjects, avg/std/min/max out-degree ---
    unique_subjects: Dict[str, int] = Counter()
    sum_edges_per_p_out: Dict[str, int] = Counter()
    subj_count_per_p_out: Dict[str, int] = Counter()
    out_deg_sumsq: Dict[str, float] = Counter()
    out_deg_min: Dict[str, int] = {}
    out_deg_max: Dict[str, int] = {}

    cur_p, cur_s, cur_ps_count = None, None, 0
    for line in merge_sorted_chunks(ps_chunks):
        p, s = line.rstrip('\n').split('\t', 1)
        if (p != cur_p) or (s != cur_s):
            if cur_p is not None and cur_s is not None:
                sum_edges_per_p_out[cur_p] += cur_ps_count
                subj_count_per_p_out[cur_p] += 1
                unique_subjects[cur_p] += 1
                out_deg_sumsq[cur_p] += (cur_ps_count * cur_ps_count)
                out_deg_min[cur_p] = cur_ps_count if cur_p not in out_deg_min else min(out_deg_min[cur_p], cur_ps_count)
                out_deg_max[cur_p] = cur_ps_count if cur_p not in out_deg_max else max(out_deg_max[cur_p], cur_ps_count)
            cur_p, cur_s, cur_ps_count = p, s, 1
        else:
            cur_ps_count += 1
    if cur_p is not None and cur_s is not None:
        sum_edges_per_p_out[cur_p] += cur_ps_count
        subj_count_per_p_out[cur_p] += 1
        unique_subjects[cur_p] += 1
        out_deg_sumsq[cur_p] += (cur_ps_count * cur_ps_count)
        out_deg_min[cur_p] = cur_ps_count if cur_p not in out_deg_min else min(out_deg_min[cur_p], cur_ps_count)
        out_deg_max[cur_p] = cur_ps_count if cur_p not in out_deg_max else max(out_deg_max[cur_p], cur_ps_count)

    # --- (B) po: unique_objects, avg/std/min/max in-degree ---
    unique_objects: Dict[str, int] = Counter()
    sum_edges_per_p_in: Dict[str, int] = Counter()
    obj_count_per_p_in: Dict[str, int] = Counter()
    in_deg_sumsq: Dict[str, float] = Counter()
    in_deg_min: Dict[str, int] = {}
    in_deg_max: Dict[str, int] = {}

    cur_p, cur_o, cur_po_count = None, None, 0
    for line in merge_sorted_chunks(po_chunks):
        p, o = line.rstrip('\n').split('\t', 1)
        if (p != cur_p) or (o != cur_o):
            if cur_p is not None and cur_o is not None:
                sum_edges_per_p_in[cur_p] += cur_po_count
                obj_count_per_p_in[cur_p] += 1
                unique_objects[cur_p] += 1
                in_deg_sumsq[cur_p] += (cur_po_count * cur_po_count)
                in_deg_min[cur_p] = cur_po_count if cur_p not in in_deg_min else min(in_deg_min[cur_p], cur_po_count)
                in_deg_max[cur_p] = cur_po_count if cur_p not in in_deg_max else max(in_deg_max[cur_p], cur_po_count)
            cur_p, cur_o, cur_po_count = p, o, 1
        else:
            cur_po_count += 1
    if cur_p is not None and cur_o is not None:
        sum_edges_per_p_in[cur_p] += cur_po_count
        obj_count_per_p_in[cur_p] += 1
        unique_objects[cur_p] += 1
        in_deg_sumsq[cur_p] += (cur_po_count * cur_po_count)
        in_deg_min[cur_p] = cur_po_count if cur_p not in in_deg_min else min(in_deg_min[cur_p], cur_po_count)
        in_deg_max[cur_p] = cur_po_count if cur_p not in in_deg_max else max(in_deg_max[cur_p], cur_po_count)

    # --- (C) sp: N_subjects_all and co-occurrences Top-K (by data tokens) ---
    N_subjects_all = 0
    pair_counts: Dict[Tuple[str, str], int] = Counter()
    pred_support_top: Dict[str, int] = Counter()

    cur_s, cur_preds = None, set()
    for line in merge_sorted_chunks(sp_chunks):
        s, p = line.rstrip('\n').split('\t', 1)
        key_pred = p if ((not top_pred_tokens) or (p in top_pred_tokens)) else None
        if s != cur_s:
            if cur_s is not None:
                N_subjects_all += 1
                if cur_preds:
                    preds_list = sorted(cur_preds)
                    if args.max_preds_per_subject and len(preds_list) > args.max_preds_per_subject:
                        preds_list = preds_list[:args.max_preds_per_subject]
                    for pp in set(preds_list):
                        pred_support_top[pp] += 1
                    for a_i in range(len(preds_list)):
                        for b_i in range(a_i + 1, len(preds_list)):
                            a, b = preds_list[a_i], preds_list[b_i]
                            if a == b:
                                continue
                            if a < b:
                                pair_counts[(a, b)] += 1
                            else:
                                pair_counts[(b, a)] += 1
            cur_s = s
            cur_preds = set()
            if key_pred:
                cur_preds.add(key_pred)
        else:
            if key_pred:
                cur_preds.add(key_pred)

    if cur_s is not None:
        N_subjects_all += 1
        if cur_preds:
            preds_list = sorted(cur_preds)
            if args.max_preds_per_subject and len(preds_list) > args.max_preds_per_subject:
                preds_list = preds_list[:args.max_preds_per_subject]
            for pp in set(preds_list):
                pred_support_top[pp] += 1
            for a_i in range(len(preds_list)):
                for b_i in range(a_i + 1, len(preds_list)):
                    a, b = preds_list[a_i], preds_list[b_i]
                    if a == b:
                        continue
                    if a < b:
                        pair_counts[(a, b)] += 1
                    else:
                        pair_counts[(b, a)] += 1

    # --- (D) pso: unique (s,o) and duplicates (aggregated by *display* for interesting predicates) ---
    unique_so_per_p: Dict[str, int] = Counter()
    dup_excess_per_p: Dict[str, int] = Counter()
    if pso_chunks:
        cur_p, cur_s, cur_o = None, None, None
        cur_count = 0
        for line in merge_sorted_chunks(pso_chunks):
            p, s, o = line.rstrip('\n').split('\t', 2)
            if (p != cur_p) or (s != cur_s) or (o != cur_o):
                if cur_p is not None:
                    if cur_count > 0:
                        unique_so_per_p[cur_p] += 1
                        if cur_count > 1:
                            dup_excess_per_p[cur_p] += (cur_count - 1)
                cur_p, cur_s, cur_o = p, s, o
                cur_count = 1
            else:
                cur_count += 1
        if cur_p is not None:
            unique_so_per_p[cur_p] += 1
            if cur_count > 1:
                dup_excess_per_p[cur_p] += (cur_count - 1)

    # --- Write 2_per_predicate_basic.csv (global, without aliasing) ---
    with open(os.path.join(args.outdir, '2_per_predicate_basic.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow([
            'predicate', 'count', 'share',
            'unique_subjects', 'unique_objects',
            'avg_out_degree_p', 'avg_in_degree_p',
            'pct_subjects_with_p',
            'unique_s_o_pairs', 'duplicate_excess'
        ])
        for p, c in sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True):
            share = (c / N_triples) if N_triples else 0.0
            us = unique_subjects.get(p, 0)
            uo = unique_objects.get(p, 0)
            avg_out = (sum_edges_per_p_out.get(p, 0) / us) if us > 0 else 0.0
            avg_in  = (sum_edges_per_p_in.get(p, 0) / obj_count_per_p_in.get(p, 0)) if obj_count_per_p_in.get(p, 0) else 0.0
            pct_subj = (us / N_subjects_all * 100.0) if N_subjects_all else 0.0
            uso = unique_so_per_p.get(p, '') if pso_chunks and not interesting_display else ''
            dexc = dup_excess_per_p.get(p, '') if pso_chunks and (args.duplicate_excess and not interesting_display) else ''
            w.writerow([p, c, share, us, uo, avg_out, avg_in, pct_subj, uso, dexc])

    # --- Write 3 and 4 (global, by data tokens) ---
    with open(os.path.join(args.outdir, '3_predicate_cooccurrence.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow(['p_a','p_b','n_ab','n_a','n_b','support_ab','jaccard','pmi','conf(A->B)','conf(B->A)','lift(A->B)','lift(B->A)'])
        for (a, b), n_ab in sorted(pair_counts.items(), key=lambda kv: kv[1], reverse=True):
            n_a = pred_support_top.get(a, 0)
            n_b = pred_support_top.get(b, 0)
            support_ab = (n_ab / N_subjects_all) if N_subjects_all else 0.0
            support_a  = (n_a  / N_subjects_all) if N_subjects_all else 0.0
            support_b  = (n_b  / N_subjects_all) if N_subjects_all else 0.0
            denom = (n_a + n_b - n_ab)
            jacc = (n_ab / denom) if denom > 0 else 0.0
            pmi = math.log2(support_ab / (support_a * support_b)) if support_a > 0 and support_b > 0 and support_ab > 0 else 0.0
            conf_ab = (n_ab / n_a) if n_a > 0 else 0.0
            conf_ba = (n_ab / n_b) if n_b > 0 else 0.0
            lift_ab = (conf_ab / support_b) if support_b > 0 else 0.0
            lift_ba = (conf_ba / support_a) if support_a > 0 else 0.0
            w.writerow([a, b, n_ab, n_a, n_b, support_ab, jacc, pmi, conf_ab, conf_ba, lift_ab, lift_ba])

    with open(os.path.join(args.outdir, '4_association_rules.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow(['p_a','p_b','support_ab','conf(A->B)','lift(A->B)','jaccard','pmi'])
        for (a, b), n_ab in sorted(pair_counts.items(), key=lambda kv: kv[1], reverse=True):
            n_a = pred_support_top.get(a, 0)
            n_b = pred_support_top.get(b, 0)
            if N_subjects_all == 0 or n_a == 0 or n_b == 0:
                continue
            support_ab = n_ab / N_subjects_all
            support_b  = n_b / N_subjects_all
            conf_ab = n_ab / n_a
            if support_ab >= args.min_support and conf_ab >= args.min_conf:
                denom = (n_a + n_b - n_ab)
                jacc = (n_ab / denom) if denom > 0 else 0.0
                pmi = math.log2(support_ab / ((n_a/N_subjects_all)*(n_b/N_subjects_all))) if support_ab > 0 else 0.0
                lift_ab = conf_ab / support_b if support_b > 0 else 0.0
                w.writerow([a, b, support_ab, conf_ab, lift_ab, jacc, pmi])

    # --- Centralities (Top-K) — so far (by data tokens) ---
    try:
        import networkx as nx  # type: ignore
        G = nx.Graph()
        for p in top_pred_tokens:
            G.add_node(p, support=pred_support_top.get(p, 0))
        for (a, b), wgt in pair_counts.items():
            if a in top_pred_tokens and b in top_pred_tokens:
                G.add_edge(a, b, weight=wgt)
        deg_cent = nx.degree_centrality(G)
        weighted_deg = {n: sum(d.get('weight', 1) for _, _, d in G.edges(n, data=True)) for n in G.nodes()}
        pagerank = nx.pagerank(G, weight='weight') if G.number_of_edges() > 0 else {n: 0.0 for n in G.nodes()}
        with open(os.path.join(args.outdir, '5_per_predicate_graph_metrics.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['predicate','deg_centrality','weighted_degree','pagerank'])
            for p in sorted(G.nodes()):
                w.writerow([p, deg_cent.get(p, 0.0), weighted_deg.get(p, 0.0), pagerank.get(p, 0.0)])
    except ImportError:
        pass

    # --- GENERAL VISUALIZATIONS ---
    if args.plots:
        if matplotlib is None:
            print('[WARN] matplotlib not available — skipping plot generation.', file=sys.stderr)
        else:
            prefer_curie = bool(args.plots_curie_labels)
            plot_hist_counts(pred_counts, args.outdir, args.plots_dpi)
            plot_bar_top_predicates(pred_counts, args.outdir, args.plots_topN, args.plots_dpi, prefixes, prefer_curie)

            # Co-occurrence heatmap: choose Top-N by pred_support_top (number of subjects with given p)
            if pred_support_top:
                preds_sorted = sorted(pred_support_top.items(), key=lambda kv: kv[1], reverse=True)
                preds_for_heat = [p for p, _ in preds_sorted[:max(1, args.plots_topN)]]
                plot_heatmap_cooccurrence(preds_for_heat,
                                          pair_counts,
                                          pred_support_top,
                                          args.outdir, args.plots_dpi,
                                          prefixes, prefer_curie,
                                          filename=f'plot_3_cooccurrence_heatmap_top{len(preds_for_heat)}.png')

    # ===== Files for interesting predicates (8–13) — by *display* and aggregated over aliases =====
    if interesting_display:
        # Helper aggregations over *display* from maps keyed by data tokens
        def sum_over(d: Dict[str, float], keys: Set[str]) -> float:
            return float(sum(d.get(k, 0.0) for k in keys))
        def min_over(d: Dict[str, int], keys: Set[str]) -> int:
            vals = [d.get(k) for k in keys if k in d]
            return min(vals) if vals else 0
        def max_over(d: Dict[str, int], keys: Set[str]) -> int:
            vals = [d.get(k) for k in keys if k in d]
            return max(vals) if vals else 0

        # 8 — summary
        with open(os.path.join(args.outdir, '8_interesting_predicates_summary.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow([
                'predicate','count','share',
                'unique_subjects','unique_objects',
                'avg_out_degree_p','std_out_degree_p','min_out_degree_p','max_out_degree_p',
                'avg_in_degree_p','std_in_degree_p','min_in_degree_p','max_in_degree_p',
                'pct_subjects_with_p',
                'unique_s_o_pairs','duplicate_excess',
                'subj_IRI','subj_BNODE','obj_IRI','obj_BNODE','obj_LITERAL',
                'literals_total','unique_datatypes','unique_langs'
            ])
            for disp in sorted(interesting_display):
                data_keys = disp2data.get(disp, {disp})
                # count/share
                c = int(sum(pred_counts.get(k, 0) for k in data_keys))
                share = (c / N_triples) if N_triples else 0.0
                # unique S/O and OUT degrees
                us = int(sum(unique_subjects.get(k, 0) for k in data_keys))
                sum_out = sum_over(sum_edges_per_p_out, data_keys)
                n_subj = int(sum(subj_count_per_p_out.get(k, 0) for k in data_keys))
                avg_out = (sum_out / n_subj) if n_subj > 0 else 0.0
                var_out = (sum_over(out_deg_sumsq, data_keys) / n_subj - (avg_out ** 2)) if n_subj > 0 else 0.0
                std_out = math.sqrt(max(var_out, 0.0))
                min_out = min_over(out_deg_min, data_keys)
                max_out = max_over(out_deg_max, data_keys)
                # IN
                uo = int(sum(unique_objects.get(k, 0) for k in data_keys))
                sum_in = sum_over(sum_edges_per_p_in, data_keys)
                n_obj = int(sum(obj_count_per_p_in.get(k, 0) for k in data_keys))
                avg_in = (sum_in / n_obj) if n_obj > 0 else 0.0
                var_in = (sum_over(in_deg_sumsq, data_keys) / n_obj - (avg_in ** 2)) if n_obj > 0 else 0.0
                std_in = math.sqrt(max(var_in, 0.0))
                min_in = min_over(in_deg_min, data_keys)
                max_in = max_over(in_deg_max, data_keys)
                # others
                pct_subj = (us / N_subjects_all * 100.0) if N_subjects_all else 0.0
                uso = int(unique_so_per_p.get(disp, 0))
                dexc = int(dup_excess_per_p.get(disp, 0))
                sk = subj_kind_counts.get(disp, Counter())
                ok = obj_kind_counts.get(disp, Counter())
                lit_total = int(literals_total.get(disp, 0))
                w.writerow([
                    disp, c, share,
                    us, uo,
                    avg_out, std_out, min_out, max_out,
                    avg_in,  std_in, min_in, max_in,
                    pct_subj,
                    uso, dexc,
                    sk.get('IRI', 0), sk.get('BNODE', 0), ok.get('IRI', 0), ok.get('BNODE', 0), ok.get('LITERAL', 0),
                    lit_total, len(datatype_counts.get(disp, {})), len(lang_counts.get(disp, {}))
                ])

        # 9 — datatypes
        with open(os.path.join(args.outdir, '9_interesting_predicates_datatypes.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['predicate','datatype','count','share_among_literals'])
            for disp in sorted(interesting_display):
                lit_tot = max(literals_total.get(disp, 0), 1)
                for dt, cnt in sorted(datatype_counts.get(disp, {}).items(), key=lambda kv: kv[1], reverse=True):
                    w.writerow([disp, dt, cnt, cnt / lit_tot])

        # 10 — languages
        with open(os.path.join(args.outdir, '10_interesting_predicates_langs.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['predicate','lang','count','share_among_literals'])
            for disp in sorted(interesting_display):
                lit_tot = max(literals_total.get(disp, 0), 1)
                for lg, cnt in sorted(lang_counts.get(disp, {}).items(), key=lambda kv: kv[1], reverse=True):
                    w.writerow([disp, lg, cnt, cnt / lit_tot])

        # 11 — Top object IRIs
        with open(os.path.join(args.outdir, '11_interesting_predicates_top_objects.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['predicate','object_iri','count','share_among_predicate'])
            for disp in sorted(interesting_display):
                total_p = max(int(sum(pred_counts.get(k, 0) for k in disp2data.get(disp, {disp}))), 1)
                topN = obj_top_iris.get(disp, Counter()).most_common(args.top_values)
                for obj_iri, cnt in topN:
                    w.writerow([disp, obj_iri, cnt, cnt / total_p])

        # Prepare co-occurrence counts *after aliasing* (pairs and supports by display)
        tok2disp = (lambda t: alias_rev.get(t, t))
        pred_support_disp: Dict[str, int] = Counter()
        for t, v in pred_support_top.items():
            pred_support_disp[tok2disp(t)] += v
        pair_counts_disp: Dict[Tuple[str, str], int] = Counter()
        for (a, b), v in pair_counts.items():
            A = tok2disp(a)
            B = tok2disp(b)
            if A <= B:
                pair_counts_disp[(A, B)] += v
            else:
                pair_counts_disp[(B, A)] += v

        # 12 — co-occurrences involving interesting predicates (after aliasing)
        with open(os.path.join(args.outdir, '12_interesting_predicates_cooccurrence.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['p_a','p_b','n_ab','n_a','n_b','support_ab','jaccard','pmi','conf(A->B)','conf(B->A)','lift(A->B)','lift(B->A)'])
            for (a, b), n_ab in sorted(pair_counts_disp.items(), key=lambda kv: kv[1], reverse=True):
                if (a not in interesting_display) and (b not in interesting_display):
                    continue
                n_a = pred_support_disp.get(a, 0)
                n_b = pred_support_disp.get(b, 0)
                support_ab = (n_ab / N_subjects_all) if N_subjects_all else 0.0
                support_a  = (n_a  / N_subjects_all) if N_subjects_all else 0.0
                support_b  = (n_b  / N_subjects_all) if N_subjects_all else 0.0
                denom = (n_a + n_b - n_ab)
                jacc = (n_ab / denom) if denom > 0 else 0.0
                pmi = math.log2(support_ab / (support_a * support_b)) if support_a > 0 and support_b > 0 and support_ab > 0 else 0.0
                conf_ab = (n_ab / n_a) if n_a > 0 else 0.0
                conf_ba = (n_ab / n_b) if n_b > 0 else 0.0
                lift_ab = (conf_ab / support_b) if support_b > 0 else 0.0
                lift_ba = (conf_ba / support_a) if support_a > 0 else 0.0
                w.writerow([a, b, n_ab, n_a, n_b, support_ab, jacc, pmi, conf_ab, conf_ba, lift_ab, lift_ba])

        # 13 — rules A->B with an interesting antecedent (after aliasing)
        with open(os.path.join(args.outdir, '13_interesting_predicates_rules.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['p_a','p_b','support_ab','conf(A->B)','lift(A->B)','jaccard','pmi'])
            for (a, b), n_ab in sorted(pair_counts_disp.items(), key=lambda kv: kv[1], reverse=True):
                if a not in interesting_display:
                    continue
                n_a = pred_support_disp.get(a, 0)
                n_b = pred_support_disp.get(b, 0)
                if N_subjects_all == 0 or n_a == 0 or n_b == 0:
                    continue
                support_ab = n_ab / N_subjects_all
                support_b  = n_b / N_subjects_all
                conf_ab = n_ab / n_a
                if support_ab >= args.min_support and conf_ab >= args.min_conf:
                    denom = (n_a + n_b - n_ab)
                    jacc = (n_ab / denom) if denom > 0 else 0.0
                    pmi = math.log2(support_ab / ((n_a/N_subjects_all)*(n_b/N_subjects_all))) if support_ab > 0 else 0.0
                    lift_ab = conf_ab / support_b if support_b > 0 else 0.0
                    w.writerow([a, b, support_ab, conf_ab, lift_ab, jacc, pmi])

        # --- VISUALIZATIONS FOR INTERESTING PREDICATES (after aliasing) ---
        if args.plots and matplotlib is not None:
            prefer_curie = bool(args.plots_curie_labels)
            preds_i = sorted(list(interesting_display),
                             key=lambda p: pred_support_disp.get(p, 0),
                             reverse=True)
            if preds_i:
                preds_i = preds_i[:max(1, args.plots_topN)]
                plot_heatmap_cooccurrence(preds_i,
                                          pair_counts_disp,
                                          pred_support_disp,
                                          args.outdir, args.plots_dpi,
                                          prefixes, prefer_curie,
                                          filename='plot_12_interesting_cooccurrence_heatmap.png')

    print('[OK] Done. Results written to:', args.outdir)
    tmp_ctx.cleanup()


# Helper nullcontext for conditional 'with'
class nullcontext:
    def __enter__(self): return io.StringIO()  # dummy writer
    def __exit__(self, exc_type, exc, tb): return False


if __name__ == '__main__':
    main()

