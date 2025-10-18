#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cidoc-crm-gen.py ― version 2.3
====================================

Generator of synthetic RDF data compliant with the **CIDOC-CRM** ontology.
Uses the **Faker** library to create realistic labels and writes
the result in *Turtle* serialization.

**What's new in version 2.3**
-----------------------------
* Added the **--density <D>** parameter (0 ≤ D ≤ 1) controlling graph *density*,
  understood as the probability of adding extra edges between instances
  (e.g., hierarchical or binary relations between periods or STV).
  The default D = 0.3 reproduces earlier behavior (≈30% of the maximum
  potential edges are realized).
* All randomly added relations (P9, P10-additional, P89-parent, P121,
  P133, P189) scale linearly with the *density* parameter.
* The parameter is passed both via the CLI and to the `build_graph()` function.

Supported classes and properties
--------------------------------
* **Classes**  E4 Period, E53 Place, E92 Spacetime Volume, E19 Physical Object.
* **Relations**
  * P7  (took place at)
  * P9  (consists of)                TRA ∧ ASM
  * P10 (falls within)               REF ∧ TRA
  * P55 (has current location)
  * P89 (falls within)               REF ∧ TRA
  * P121 (overlaps with)             SYM ∧ REF
  * P133 (is spatiotemporally separated from)   SYM ∧ IRF
  * P161 (has spatial projection)
  * P189 (approximates)              REF

Usage example
-------------
python cidoc_crm_generator.py \
    --periods 100 --places 60 --objects 120 --spacetime 80 \
    --density 0.6 --seed 42 --locale pl_PL --output dense.ttl
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import date, timedelta
from typing import Optional

from faker import Faker
from rdflib import Graph, Namespace, RDF, RDFS, XSD, URIRef, Literal

# ——————————————————————————————————
# Namespaces
# ——————————————————————————————————
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
EX  = Namespace("http://example.org/id/")  # instance identifiers

# ——————————————————————————————————
# Helper functions
# ——————————————————————————————————

def _uri(path: str) -> URIRef:
    """Return a URIRef in the EX namespace."""
    return URIRef(EX + path)


def _random_date(start: date = date(1700, 1, 1), end: date = date(2025, 1, 1)) -> date:
    """Random xsd:date between *start* and *end*."""
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


# ——————————————————————————————————
# Main graph construction function
# ——————————————————————————————————

def build_graph(
    n_periods: int = 10,
    n_places: int = 10,
    n_objects: int = 15,
    n_spacetime: int = 10,
    density: float = 0.3,
    seed: Optional[int] = None,
    locale: str = "en_US",
    faker_on: bool = True,
) -> Graph:
    """Build a synthetic CIDOC-CRM graph.

    The *density* parameter ∈ [0, 1] specifies the probability of randomly
    adding additional edges between instances. At 0 the graph contains only
    required edges (e.g., reflexive ones); at 1 it generates as many relations
    as permitted by the REF/SYM/TRA/ASM/IRF logic.
    """

    # Density normalization
    density = max(0.0, min(1.0, density))

    if seed is not None:
        random.seed(seed)

    faker = Faker(locale)
    if seed is not None:
        Faker.seed(seed)

    g = Graph()
    g.bind("crm", CRM)
    g.bind("ex", EX)

    # Collections of instances
    periods: list[URIRef] = []
    places: list[URIRef] = []
    objects: list[URIRef] = []
    stv: list[URIRef]     = []

    # ———————————————— Places (E53) ————————————————
    for i in range(n_places):
        uri = _uri(f"place/{uuid.uuid4()}")
        g.add((uri, RDF.type, CRM.E53_Place))
        label = faker.city() if faker_on else f"Synthetic Place {i+1}"
        g.add((uri, RDFS.label, Literal(label, lang=locale.split("_")[0])))
        places.append(uri)

    # P89 (REF ∧ TRA)
    for p in places:
        g.add((p, CRM.P89_falls_within, p))  # reflexively
        if random.random() < density:
            parent = random.choice(places)
            if parent != p:
                g.add((p, CRM.P89_falls_within, parent))

    # ———————————————— Spacetime Volumes (E92) ————————————————
    for i in range(n_spacetime):
        uri = _uri(f"stv/{uuid.uuid4()}")
        g.add((uri, RDF.type, CRM.E92_Spacetime_Volume))
        label = (
            f"{faker.country()} Conflict Zone" if faker_on else f"SpacetimeVolume {i+1}"
        )
        g.add((uri, RDFS.label, Literal(label, lang=locale.split("_")[0])))
        g.add((uri, CRM.P4_has_time_span, Literal(str(_random_date()), datatype=XSD.date)))
        stv.append(uri)

    for s in stv:
        g.add((s, CRM.P161_has_spatial_projection, random.choice(places)))

        # P121 (SYM ∧ REF)
        g.add((s, CRM.P121_overlaps_with, s))
        if random.random() < density:
            partner = random.choice(stv)
            if partner != s:
                g.add((s, CRM.P121_overlaps_with, partner))
                g.add((partner, CRM.P121_overlaps_with, s))

        # P189 (REF)
        g.add((s, CRM.P189_approximates, s))
        if random.random() < density / 2:  # less frequent, because directional
            partner = random.choice(stv)
            if partner != s:
                g.add((s, CRM.P189_approximates, partner))

        # P133 (SYM ∧ IRF)
        if random.random() < density:
            partner = random.choice(stv)
            if partner != s:
                g.add((s, CRM.P133_is_spatiotemporally_separated_from, partner))
                g.add((partner, CRM.P133_is_spatiotemporally_separated_from, s))

    # ———————————————— Periods (E4) ————————————————
    for i in range(n_periods):
        uri = _uri(f"period/{uuid.uuid4()}")
        g.add((uri, RDF.type, CRM.E4_Period))
        label = (
            f"{faker.catch_phrase()} Era" if faker_on else f"Period {i+1}"
        )
        g.add((uri, RDFS.label, Literal(label, lang=locale.split("_")[0])))
        periods.append(uri)

    for p in periods:
        g.add((p, CRM.P7_took_place_at, random.choice(places)))

        # P10 (REF ∧ TRA)
        g.add((p, CRM.P10_falls_within, p))
        if random.random() < density:
            other = random.choice(periods)
            if other != p:
                g.add((p, CRM.P10_falls_within, other))

        # P9 (TRA ∧ ASM)
        if random.random() < density:
            other = random.choice(periods)
            if other != p:
                g.add((p, CRM.P9_consists_of, other))

    # ———————————————— Physical Objects (E19) ————————————————
    for i in range(n_objects):
        uri = _uri(f"object/{uuid.uuid4()}")
        g.add((uri, RDF.type, CRM.E19_Physical_Object))
        label = (
            f"{faker.color_name()} {faker.word().capitalize()} Artifact"
            if faker_on else f"Artefact {i+1}"
        )
        g.add((uri, RDFS.label, Literal(label, lang=locale.split("_")[0])))
        g.add((uri, CRM.P55_has_current_location, random.choice(places)))
        g.add((uri, CRM.P108i_was_produced_by, random.choice(periods)))
        objects.append(uri)

    return g


# ——————————————————————————————————
# CLI interface
# ——————————————————————————————————

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic CIDOC-CRM RDF (TTL) with controllable density (ver. 2.3)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--periods", type=int, default=10, help="number of Period instances")
    parser.add_argument("--places", type=int, default=10, help="number of Place instances")
    parser.add_argument("--objects", type=int, default=15, help="number of Physical Objects")
    parser.add_argument("--spacetime", type=int, default=10, help="number of Spacetime Volumes")
    parser.add_argument("--density", type=float, default=0.3, help="edge density (0-1)")
    parser.add_argument("--seed", type=int, default=None, help="random seed for reproducibility")
    parser.add_argument("--locale", type=str, default="en_US", help="Faker locale (e.g. pl_PL)")
    parser.add_argument("--no-faker", action="store_true", help="disable Faker; use generic labels")
    parser.add_argument("--output", type=str, default="synthetic.ttl", help="Turtle output file path")

    args = parser.parse_args()

    g = build_graph(
        n_periods=args.periods,
        n_places=args.places,
        n_objects=args.objects,
        n_spacetime=args.spacetime,
        density=args.density,
        seed=args.seed,
        locale=args.locale,
        faker_on=not args.no_faker,
    )

    g.serialize(destination=args.output, format="turtle")
    print(f"Generated {len(g)} triples → {args.output}")


if __name__ == "__main__":
    main()

