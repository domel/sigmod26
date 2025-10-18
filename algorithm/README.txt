Description
-----------
Computes **abstraction (indistinguishability) classes** for sets of binary-relation
properties based on the given contradictions and implications. For every subset of
properties the script computes the implicational closure; contradictory sets are
collected into a separate class.

Requirements
------------
- Python 3.8+ (standard library only)

Usage
-----
- Human-readable output:
  python rel_classes.py

- CSV to stdout:
  python rel_classes.py --csv

Output
------
- Human-readable: lines of the form "<class_no>: {set1}, {set2}, ...",
  followed by the total number of classes.
- CSV: each row starts with the class number followed by the sets in that class;
  the last row is "count,<num_classes>".
- Empty set is printed as "Ø".

Property glossary (code → meaning)
----------------------------------
REF → reflexive
SYM → symmetric
TRA → transitive
IRF → irreflexive
ANS → antisymmetric
ASM → asymmetric
ACC → acyclic
ITR → intransitive
ANT → antitransitive

Notes
-----
The candidate space has size 2^9 = 512. Indistinguishability checks closures
against all valid candidate subsets.

