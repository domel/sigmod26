#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
neo_pg_stats.py — Lightweight, streaming statistics counter for CSV (Neo4j-style),
with a filter for selected relations: --interesting 'registered_address,intermediary_of'

Input:
  - --edges relationships.csv[.gz|.bz2|.xz]
      Required columns: node_id_start, node_id_end, rel_type
  - --nodes-glob 'nodes-*.csv' (optional)
      Required column: node_id
      Node category: column 'type' (if present and non-empty) or the filename part after 'nodes-'.

Outputs (CSV):
  1_predicate_distribution.csv
  1_meta_entropy_gini.csv
  2_per_predicate_basic.csv
     Columns (in this order):
       rel_type, count, share, unique_subjects, unique_objects,
       avg_out_degree_p, std_out_degree_p, min_out_degree_p, max_out_degree_p,
       avg_in_degree_p,  std_in_degree_p,  min_in_degree_p,  max_in_degree_p
  3_predicate_cooccurrence.csv
  4_association_rules.csv
  5_per_predicate_graph_metrics.csv
  6_subject_category_counts.csv                (if --nodes-glob)
  7_object_category_counts.csv                (if --nodes-glob)

Additional (when --interesting-scope any and --interesting is provided):
  3b_predicate_cooccurrence_interesting.csv   — pairs where (A ∈ I) ∨ (B ∈ I)
  4b_association_rules_interesting.csv        — rules A→B with (A ∈ I) ∨ (B ∈ I)

Plots (PNG, optional --plots):
  - histogram of predicate counts,
  - bar chart of Top-N predicates,
  - heatmap of predicate co-occurrence (Top-N).

Author: (insert your name)
Version: 1.0 (2025-09-16)
"""

import argparse
import bz2
import csv
import gzip
import heapq
import io
import itertools
import lzma
import math
import os
import re
import sys
import tempfile
import glob
from collections import Counter, defaultdict
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Set

# ----------------------
# I/O helpers
# ----------------------

def open_maybe_compressed(path: str) -> io.TextIOBase:
    lower = path.lower()
    if lower.endswith('.gz'):
        return io.TextIOWrapper(gzip.open(path, 'rb'), encoding='utf-8', errors='replace', newline='')
    if lower.endswith('.bz2'):
        return io.TextIOWrapper(bz2.open(path, 'rb'), encoding='utf-8', errors='replace', newline='')
    if lower.endswith('.xz') or lower.endswith('.lzma'):
        return io.TextIOWrapper(lzma.open(path, 'rb'), encoding='utf-8', errors='replace', newline='')
    return open(path, 'r', encoding='utf-8', errors='replace', newline='')

def chunked_sorted_files(raw_path: str, chunk_lines: int) -> List[str]:
    """External sort: splits the file into sorted chunks."""
    chunk_paths: List[str] = []
    if not raw_path or not os.path.exists(raw_path):
        return chunk_paths
    with open(raw_path, 'r', encoding='utf-8', errors='replace') as f:
        buf: List[str] = []
        for line in f:
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
# Helper metrics
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
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:
    matplotlib = None  # no matplotlib -> skip plots

def plot_hist_counts(pred_counts: Dict[str, int], outdir: str, dpi: int):
    if matplotlib is None or not pred_counts:
        return
    counts = list(pred_counts.values())
    if not counts:
        return
    plt.figure(figsize=(8, 5))
    plt.hist(counts, bins='auto')
    plt.title('Histogram of relation (predicate) counts')
    plt.xlabel('Number of relation occurrences (count)')
    plt.ylabel('Number of distinct relations')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, 'plot_1a_predicate_count_histogram.png'), dpi=dpi)
    plt.close()

def plot_bar_top_predicates(pred_counts: Dict[str, int], outdir: str, topN: int, dpi: int):
    if matplotlib is None or not pred_counts:
        return
    items = sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True)[:max(1, topN)]
    labels = [p for p, _ in items]
    values = [c for _, c in items]
    width = max(9.0, 0.45 * len(labels))
    plt.figure(figsize=(width, 6))
    plt.bar(range(len(values)), values)
    plt.xticks(range(len(labels)), labels, rotation=80, ha='right')
    plt.title(f'Top-{len(labels)} relations (count)')
    plt.xlabel('rel_type')
    plt.ylabel('Count')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, f'plot_1b_top_predicates_bar_top{len(labels)}.png'), dpi=dpi)
    plt.close()

def plot_heatmap_cooccurrence(preds: List[str],
                              pair_counts: Dict[Tuple[str, str], int],
                              pred_support: Dict[str, int],
                              outdir: str, dpi: int,
                              filename: str):
    if matplotlib is None or not preds:
        return
    k = len(preds)
    idx = {p: i for i, p in enumerate(preds)}
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
    side = max(6.0, 0.40 * k)
    plt.figure(figsize=(side, side))
    im = plt.imshow(M, aspect='auto', interpolation='nearest')
    plt.colorbar(im, fraction=0.46/10, pad=0.04)
    plt.xticks(range(k), preds, rotation=80, ha='right')
    plt.yticks(range(k), preds)
    plt.title('Predicate co-occurrence (n_ab; diag: n_a)')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, filename), dpi=dpi)
    plt.close()

# ----------------------
# Helpers for node categories
# ----------------------

def derive_category_from_filename(path: str) -> str:
    base = os.path.basename(path)
    m = re.match(r'^nodes-([^.]+)\.csv$', base)
    if m:
        return m.group(1)
    return os.path.splitext(base)[0]

def stream_nodes_categories(nodes_glob: Optional[str], tmpdir: str) -> Optional[str]:
    """Builds a temporary file with a category map: node_id\tcategory\n. Returns the path or None."""
    if not nodes_glob:
        return None
    out_path = os.path.join(tmpdir, 'nodes_categories.raw')
    wrote_any = False
    with open(out_path, 'w', encoding='utf-8') as fout:
        for path in sorted(glob.glob(nodes_glob)):
            default_cat = derive_category_from_filename(path)
            try:
                with open_maybe_compressed(path) as f:
                    rdr = csv.DictReader(f)
                    if 'node_id' not in rdr.fieldnames:
                        print(f"[WARN] Skipping {path} — missing 'node_id' column.", file=sys.stderr)
                        continue
                    use_type_col = 'type' in rdr.fieldnames
                    for row in rdr:
                        nid = (row.get('node_id') or '').strip()
                        if not nid:
                            continue
                        cat = (row.get('type') or '').strip() if use_type_col else ''
                        if not cat:
                            cat = default_cat
                        fout.write(f"{nid}\t{cat}\n")
                        wrote_any = True
            except Exception as e:
                print(f"[WARN] Failed to process {path}: {e}", file=sys.stderr)
    return out_path if wrote_any else None

# ----------------------
# Main logic
# ----------------------

def parse_interesting(arg: Optional[str]) -> Set[str]:
    if not arg:
        return set()
    items = [x.strip() for x in arg.split(',')]
    return {x for x in items if x}

def main():
    ap = argparse.ArgumentParser(description='Lightweight statistics counter for relation CSV (Neo4j-style) with --interesting filter.')
    ap.add_argument('--edges', required=True, help='Path to relationships.csv (.gz/.bz2/.xz allowed).')
    ap.add_argument('--nodes-glob', default=None, help='Glob pattern for node files, e.g., "nodes-*.csv" (optional).')
    ap.add_argument('--outdir', required=True, help='Output directory for CSV/PNG.')
    ap.add_argument('--tmpdir', default=None, help='Temporary directory (system default if not provided).')
    ap.add_argument('--chunk-lines', type=int, default=500_000, help='Chunk size for external sorting (number of lines).')
    ap.add_argument('--topk', type=int, default=200, help='Top-K relations for co-occurrence and centrality.')
    ap.add_argument('--max-preds-per-subject', type=int, default=50, help='Limit of relations per source (for co-occurrence).')
    ap.add_argument('--min-support', type=float, default=0.01, help='Minimum support for rules (e.g., 0.01).')
    ap.add_argument('--min-conf', type=float, default=0.5, help='Minimum confidence for rules (e.g., 0.5).')

    # plots
    ap.add_argument('--plots', action='store_true', help='Generate PNG plots in the output directory.')
    ap.add_argument('--plots-topN', type=int, default=30, help='Number of relations in Top-N plots (default 30).')
    ap.add_argument('--plots-dpi', type=int, default=150, help='DPI for PNG outputs (default 150).')

    # --- NEW: relation filter ---
    ap.add_argument('--interesting', default=None,
                    help="Comma-separated list of relations, e.g., 'registered_address,intermediary_of'.")
    ap.add_argument('--interesting-scope', choices=['subset','any'], default='subset',
                    help="subset: process only relations from --interesting; "
                         "any: compute globally, and additionally restrict co-occurrences/rules to those involving 'interesting' relations.")

    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    tmp_ctx = tempfile.TemporaryDirectory(dir=args.tmpdir)
    tmpdir = tmp_ctx.name

    interesting_set = parse_interesting(args.interesting)
    use_subset_filter = bool(interesting_set) and (args.interesting_scope == 'subset')
    use_any_overlay   = bool(interesting_set) and (args.interesting_scope == 'any')

    # RAW files (TSV) created in streaming manner from relationships
    rs_raw = os.path.join(tmpdir, 'rs.raw')   # rel_type \t start_id
    ro_raw = os.path.join(tmpdir, 'ro.raw')   # rel_type \t end_id
    sr_raw = os.path.join(tmpdir, 'sr.raw')   # start_id \t rel_type (for co-occurrence)
    or_raw = os.path.join(tmpdir, 'or.raw')   # end_id   \t rel_type (for categories)
    rso_raw = os.path.join(tmpdir, 'rso.raw') # rel_type \t start_id \t end_id

    pred_counts: Dict[str, int] = Counter()
    N_edges = 0
    bad_rows = 0

    # Pass 0: relations → intermediate files and basic counts
    with (
        open(rs_raw, 'w', encoding='utf-8') as f_rs,
        open(ro_raw, 'w', encoding='utf-8') as f_ro,
        open(sr_raw, 'w', encoding='utf-8') as f_sr,
        open(or_raw, 'w', encoding='utf-8') as f_or,
        open(rso_raw, 'w', encoding='utf-8') as f_rso,
        open_maybe_compressed(args.edges) as fin
    ):
        rdr = csv.DictReader(fin)
        required = {'node_id_start','node_id_end','rel_type'}
        if not rdr.fieldnames or not required.issubset(set(rdr.fieldnames)):
            print(f"[ERROR] File {args.edges} must contain columns: {sorted(required)}", file=sys.stderr)
            sys.exit(2)
        for row in rdr:
            s = (row.get('node_id_start') or '').strip()
            o = (row.get('node_id_end') or '').strip()
            p = (row.get('rel_type') or '').strip()
            if not s or not o or not p:
                bad_rows += 1
                continue
            # hard filter (subset)
            if use_subset_filter and (p not in interesting_set):
                continue
            N_edges += 1
            pred_counts[p] += 1
            f_rs.write(f"{p}\t{s}\n")
            f_ro.write(f"{p}\t{o}\n")
            f_sr.write(f"{s}\t{p}\n")
            f_or.write(f"{o}\t{p}\n")
            f_rso.write(f"{p}\t{s}\t{o}\n")

    if bad_rows:
        print(f"[WARN] Skipped {bad_rows} relationship rows with missing data.", file=sys.stderr)

    # Relation distribution: entropy and gini
    shares = [c / N_edges for c in pred_counts.values()] if N_edges else []
    entropy = shannon_entropy(shares) if shares else 0.0
    gini = gini_index(shares) if shares else 0.0

    # Write 1_* CSV
    with open(os.path.join(args.outdir, '1_predicate_distribution.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow(['rel_type', 'count', 'share'])
        for p, c in sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True):
            w.writerow([p, c, (c / N_edges if N_edges else 0.0)])

    with open(os.path.join(args.outdir, '1_meta_entropy_gini.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow(['N_edges', 'N_rel_types', 'entropy_bits', 'gini'])
        w.writerow([N_edges, len(pred_counts), entropy, gini])

    # Top-K for co-occurrence
    topk_n = max(0, int(args.topk))
    top_pred_tokens = {p for p, _ in sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True)[:topk_n]} if topk_n else set()

    # External sorting
    rs_chunks  = chunked_sorted_files(rs_raw, args.chunk_lines)
    ro_chunks  = chunked_sorted_files(ro_raw, args.chunk_lines)
    sr_chunks  = chunked_sorted_files(sr_raw, args.chunk_lines)
    or_chunks  = chunked_sorted_files(or_raw, args.chunk_lines)
    rso_chunks = chunked_sorted_files(rso_raw, args.chunk_lines)

    # --- (A) rs: unique_subjects and OUT degrees per predicate ---
    unique_subjects: Dict[str, int] = Counter()
    sum_edges_per_p_out: Dict[str, int] = Counter()
    subj_count_per_p_out: Dict[str, int] = Counter()
    out_deg_sumsq: Dict[str, float] = Counter()
    out_deg_min: Dict[str, int] = {}
    out_deg_max: Dict[str, int] = {}

    cur_p, cur_s, cur_ps_count = None, None, 0
    for line in merge_sorted_chunks(rs_chunks):
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

    # --- (B) ro: unique_objects and IN degrees per predicate ---
    unique_objects: Dict[str, int] = Counter()
    sum_edges_per_p_in: Dict[str, int] = Counter()
    obj_count_per_p_in: Dict[str, int] = Counter()
    in_deg_sumsq: Dict[str, float] = Counter()
    in_deg_min: Dict[str, int] = {}
    in_deg_max: Dict[str, int] = {}

    cur_p, cur_o, cur_po_count = None, None, 0
    for line in merge_sorted_chunks(ro_chunks):
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

    # --- (C) sr: predicate co-occurrence on the same source ---
    N_subjects_all = 0
    pair_counts: Dict[Tuple[str, str], int] = Counter()
    pred_support_top: Dict[str, int] = Counter()

    cur_s, cur_preds = None, set()
    for line in merge_sorted_chunks(sr_chunks):
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
                    for i in range(len(preds_list)):
                        for j in range(i + 1, len(preds_list)):
                            a, b = preds_list[i], preds_list[j]
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
            for i in range(len(preds_list)):
                for j in range(i + 1, len(preds_list)):
                    a, b = preds_list[i], preds_list[j]
                    if a == b:
                        continue
                    if a < b:
                        pair_counts[(a, b)] += 1
                    else:
                        pair_counts[(b, a)] += 1

    # --- (D) rso: unique (s,o) and redundant duplicates per predicate ---
    unique_so_per_p: Dict[str, int] = Counter()
    dup_excess_per_p: Dict[str, int] = Counter()
    cur_p, cur_s, cur_o = None, None, None
    cur_count = 0
    for line in merge_sorted_chunks(rso_chunks):
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

    # --- Write 2_per_predicate_basic.csv ---
    with open(os.path.join(args.outdir, '2_per_predicate_basic.csv'), 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow([
            'rel_type',
            'count',
            'share',
            'unique_subjects',
            'unique_objects',
            'avg_out_degree_p',
            'std_out_degree_p',
            'min_out_degree_p',
            'max_out_degree_p',
            'avg_in_degree_p',
            'std_in_degree_p',
            'min_in_degree_p',
            'max_in_degree_p'
        ])

        for p, c in sorted(pred_counts.items(), key=lambda kv: kv[1], reverse=True):
            share = (c / N_edges) if N_edges else 0.0

            # OUT-degree (on subjects) for predicate p
            n_out = subj_count_per_p_out.get(p, 0)
            sum_out = sum_edges_per_p_out.get(p, 0)
            sumsq_out = out_deg_sumsq.get(p, 0.0)
            avg_out = (sum_out / n_out) if n_out > 0 else 0.0
            var_out = (sumsq_out / n_out - avg_out * avg_out) if n_out > 0 else 0.0
            if var_out < 0.0:
                var_out = 0.0
            std_out = math.sqrt(var_out)
            min_out = out_deg_min.get(p, 0)
            max_out = out_deg_max.get(p, 0)

            # IN-degree (on objects) for predicate p
            n_in = obj_count_per_p_in.get(p, 0)
            sum_in = sum_edges_per_p_in.get(p, 0)
            sumsq_in = in_deg_sumsq.get(p, 0.0)
            avg_in  = (sum_in / n_in) if n_in > 0 else 0.0
            var_in  = (sumsq_in / n_in - avg_in * avg_in) if n_in > 0 else 0.0
            if var_in < 0.0:
                var_in = 0.0
            std_in  = math.sqrt(var_in)
            min_in = in_deg_min.get(p, 0)
            max_in = in_deg_max.get(p, 0)

            w.writerow([
                p,
                c,
                share,
                unique_subjects.get(p, 0),
                unique_objects.get(p, 0),
                avg_out,
                std_out,
                min_out,
                max_out,
                avg_in,
                std_in,
                min_in,
                max_in
            ])

    # --- Write 3 and 4 (co-occurrence on sources) ---
    def write_cooc_rules(path3: str, path4: str,
                         pair_counts_in: Dict[Tuple[str, str], int],
                         pred_support_in: Dict[str, int],
                         N_subj: int):
        with open(path3, 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['p_a','p_b','n_ab','n_a','n_b','support_ab','jaccard','pmi','conf(A->B)','conf(B->A)','lift(A->B)','lift(B->A)'])
            for (a, b), n_ab in sorted(pair_counts_in.items(), key=lambda kv: kv[1], reverse=True):
                n_a = pred_support_in.get(a, 0)
                n_b = pred_support_in.get(b, 0)
                if N_subj == 0:
                    continue
                support_ab = n_ab / N_subj
                support_a  = n_a / N_subj
                support_b  = n_b / N_subj
                denom = (n_a + n_b - n_ab)
                jacc = (n_ab / denom) if denom > 0 else 0.0
                pmi = math.log2(support_ab / (support_a * support_b)) if support_a > 0 and support_b > 0 and support_ab > 0 else 0.0
                conf_ab = (n_ab / n_a) if n_a > 0 else 0.0
                conf_ba = (n_ab / n_b) if n_b > 0 else 0.0
                lift_ab = (conf_ab / support_b) if support_b > 0 else 0.0
                lift_ba = (conf_ba / support_a) if support_a > 0 else 0.0
                w.writerow([a, b, n_ab, n_a, n_b, support_ab, jacc, pmi, conf_ab, conf_ba, lift_ab, lift_ba])

        with open(path4, 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['p_a','p_b','support_ab','conf(A->B)','lift(A->B)','jaccard','pmi'])
            for (a, b), n_ab in sorted(pair_counts_in.items(), key=lambda kv: kv[1], reverse=True):
                n_a = pred_support_in.get(a, 0)
                n_b = pred_support_in.get(b, 0)
                if N_subj == 0 or n_a == 0 or n_b == 0:
                    continue
                support_ab = n_ab / N_subj
                support_b  = n_b / N_subj
                conf_ab = n_ab / n_a
                if support_ab >= args.min_support and conf_ab >= args.min_conf:
                    denom = (n_a + n_b - n_ab)
                    jacc = (n_ab / denom) if denom > 0 else 0.0
                    pmi = math.log2(support_ab / ((n_a/N_subj)*(n_b/N_subj))) if support_ab > 0 else 0.0
                    lift_ab = conf_ab / support_b if support_b > 0 else 0.0
                    w.writerow([a, b, support_ab, conf_ab, lift_ab, jacc, pmi])

    # Standard (for the current universe of processed edges)
    write_cooc_rules(
        os.path.join(args.outdir, '3_predicate_cooccurrence.csv'),
        os.path.join(args.outdir, '4_association_rules.csv'),
        pair_counts, pred_support_top, N_subjects_all
    )

    # Overlay “interesting-any”: filter pairs with (A ∈ I) ∨ (B ∈ I), keeping supports n_a, n_b from the current universe
    if use_any_overlay:
        pair_counts_any: Dict[Tuple[str, str], int] = {}
        for (a, b), n_ab in pair_counts.items():
            if (a in interesting_set) or (b in interesting_set):
                pair_counts_any[(a, b)] = n_ab
        write_cooc_rules(
            os.path.join(args.outdir, '3b_predicate_cooccurrence_interesting.csv'),
            os.path.join(args.outdir, '4b_association_rules_interesting.csv'),
            pair_counts_any, pred_support_top, N_subjects_all
        )

    # --- Centralities (Top-K) ---
    try:
        import networkx as nx  # type: ignore
        G = nx.Graph()
        use_nodes = (top_pred_tokens if top_pred_tokens else pred_counts.keys())
        for p in use_nodes:
            G.add_node(p, support=pred_support_top.get(p, 0))
        for (a, b), wgt in pair_counts.items():
            if (not top_pred_tokens) or (a in top_pred_tokens and b in top_pred_tokens):
                G.add_edge(a, b, weight=wgt)
        deg_cent = nx.degree_centrality(G) if G.number_of_nodes() > 0 else {}
        weighted_deg = {n: sum(d.get('weight', 1) for _, _, d in G.edges(n, data=True)) for n in G.nodes()} if G.number_of_nodes() > 0 else {}
        pagerank = nx.pagerank(G, weight='weight') if G.number_of_edges() > 0 else {n: 0.0 for n in G.nodes()}
        with open(os.path.join(args.outdir, '5_per_predicate_graph_metrics.csv'), 'w', newline='', encoding='utf-8') as fout:
            w = csv.writer(fout)
            w.writerow(['rel_type','deg_centrality','weighted_degree','pagerank'])
            for p in sorted(G.nodes()):
                w.writerow([p, deg_cent.get(p, 0.0), weighted_deg.get(p, 0.0), pagerank.get(p, 0.0)])
    except ImportError:
        pass

    # --- Node categorizations (optional) ---
    if args.nodes_glob:
        nodes_raw = stream_nodes_categories(args.nodes_glob, tmpdir)
        if nodes_raw:
            nodes_chunks = chunked_sorted_files(nodes_raw, args.chunk_lines)

            # (S) source categories
            subj_cat_counts: Dict[str, Counter] = defaultdict(Counter)
            def gen_sr():
                for line in merge_sorted_chunks(sr_chunks):
                    s, p = line.rstrip('\n').split('\t', 1)
                    yield s, p
            def gen_nodes():
                for line in merge_sorted_chunks(nodes_chunks):
                    nid, cat = line.rstrip('\n').split('\t', 1)
                    yield nid, cat

            it_sr = gen_sr()
            it_nd = gen_nodes()
            try:
                s_id, s_p = next(it_sr)
                n_id, n_cat = next(it_nd)
                while True:
                    if s_id == n_id:
                        cur_id = s_id
                        cur_cats = [n_cat]
                        while True:
                            try:
                                n_id2, n_cat2 = next(it_nd)
                                if n_id2 != cur_id:
                                    n_id, n_cat = n_id2, n_cat2
                                    break
                                else:
                                    cur_cats.append(n_cat2)
                            except StopIteration:
                                n_id, n_cat = None, None
                                break
                        while True:
                            subj_cat = cur_cats[0]
                            subj_cat_counts[s_p][subj_cat] += 1
                            try:
                                s_id2, s_p2 = next(it_sr)
                                if s_id2 != cur_id:
                                    s_id, s_p = s_id2, s_p2
                                    break
                                else:
                                    s_p = s_p2
                            except StopIteration:
                                s_id, s_p = None, None
                                break
                        if s_id is None and n_id is None:
                            break
                    elif s_id is not None and (n_id is None or s_id < n_id):
                        try:
                            s_id, s_p = next(it_sr)
                        except StopIteration:
                            s_id, s_p = None, None
                    else:
                        try:
                            n_id, n_cat = next(it_nd)
                        except StopIteration:
                            n_id, n_cat = None, None
                    if s_id is None and n_id is None:
                        break
            except StopIteration:
                pass

            with open(os.path.join(args.outdir, '6_subject_category_counts.csv'), 'w', newline='', encoding='utf-8') as fout:
                w = csv.writer(fout)
                w.writerow(['rel_type','subject_category','count','share_among_rel'])
                for p in sorted(subj_cat_counts.keys()):
                    total_p = max(pred_counts.get(p, 0), 1)
                    for cat, cnt in sorted(subj_cat_counts[p].items(), key=lambda kv: kv[1], reverse=True):
                        w.writerow([p, cat, cnt, cnt/total_p])

            # (O) object categories
            obj_cat_counts: Dict[str, Counter] = defaultdict(Counter)
            def gen_or():
                for line in merge_sorted_chunks(or_chunks):
                    o, p = line.rstrip('\n').split('\t', 1)
                    yield o, p

            nodes_chunks2 = chunked_sorted_files(nodes_raw, args.chunk_lines)
            def gen_nodes2():
                for line in merge_sorted_chunks(nodes_chunks2):
                    nid, cat = line.rstrip('\n').split('\t', 1)
                    yield nid, cat
            it_or = gen_or()
            it_nd2 = gen_nodes2()

            try:
                o_id, o_p = next(it_or)
                n_id, n_cat = next(it_nd2)
                while True:
                    if o_id == n_id:
                        cur_id = o_id
                        cur_cats = [n_cat]
                        while True:
                            try:
                                n_id2, n_cat2 = next(it_nd2)
                                if n_id2 != cur_id:
                                    n_id, n_cat = n_id2, n_cat2
                                    break
                                else:
                                    cur_cats.append(n_cat2)
                            except StopIteration:
                                n_id, n_cat = None, None
                                break
                        while True:
                            obj_cat = cur_cats[0]
                            obj_cat_counts[o_p][obj_cat] += 1
                            try:
                                o_id2, o_p2 = next(it_or)
                                if o_id2 != cur_id:
                                    o_id, o_p = o_id2, o_p2
                                    break
                                else:
                                    o_p = o_p2
                            except StopIteration:
                                o_id, o_p = None, None
                                break
                        if o_id is None and n_id is None:
                            break
                    elif o_id is not None and (n_id is None or o_id < n_id):
                        try:
                            o_id, o_p = next(it_or)
                        except StopIteration:
                            o_id, o_p = None, None
                    else:
                        try:
                            n_id, n_cat = next(it_nd2)
                        except StopIteration:
                            n_id, n_cat = None, None
                    if o_id is None and n_id is None:
                        break
            except StopIteration:
                pass

            with open(os.path.join(args.outdir, '7_object_category_counts.csv'), 'w', newline='', encoding='utf-8') as fout:
                w = csv.writer(fout)
                w.writerow(['rel_type','object_category','count','share_among_rel'])
                for p in sorted(obj_cat_counts.keys()):
                    total_p = max(pred_counts.get(p, 0), 1)
                    for cat, cnt in sorted(obj_cat_counts[p].items(), key=lambda kv: kv[1], reverse=True):
                        w.writerow([p, cat, cnt, cnt/total_p])

    # --- Plots ---
    if args.plots and matplotlib is not None:
        plot_hist_counts(pred_counts, args.outdir, args.plots_dpi)
        plot_bar_top_predicates(pred_counts, args.outdir, args.plots_topN, args.plots_dpi)
        if pred_support_top:
            preds_sorted = sorted(pred_support_top.items(), key=lambda kv: kv[1], reverse=True)
            preds_for_heat = [p for p, _ in preds_sorted[:max(1, args.plots_topN)]]
            plot_heatmap_cooccurrence(preds_for_heat, pair_counts, pred_support_top,
                                      args.outdir, args.plots_dpi,
                                      filename=f'plot_3_cooccurrence_heatmap_top{len(preds_for_heat)}.png')

    print('[OK] Done. Results saved to:', args.outdir)
    tmp_ctx.cleanup()

if __name__ == '__main__':
    main()

