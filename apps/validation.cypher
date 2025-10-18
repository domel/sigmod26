/* These Cypher snippets validate per-type binary-relation constraints encoded in r.brc (REF, SYM, TRA, IRF, ASM, ANS, ACC, ITR, ANT) by searching for counterexamples—such as missing reverse edges, self-loops, cycles, or absent a→c links—and summarizing each property with a boolean result. They are intended for property-graph validation workflows and run unchanged on Neo4j 4.x and 5.x. */

/* REF */ 
MATCH ()-[r]->()
WHERE 'REF' IN r.brc
WITH DISTINCT type(r) AS relType

CALL {
  WITH relType
  MATCH (n)-[r1]->(m)
  WHERE type(r1) = relType
    AND 'REF' IN r1.brc
  WITH relType, COLLECT(DISTINCT n) + COLLECT(DISTINCT m) AS nodes

  UNWIND nodes AS v
  WITH v, relType 
  WHERE NOT EXISTS {
    MATCH (v)-[r2]->(v)
    WHERE type(r2) = relType
      AND 'REF' IN r2.brc
  }

  RETURN COUNT(v) AS violations
}

RETURN relType          AS relation,
       violations = 0   AS isReflexive;

/* SYM */
MATCH (a)-[r]->(b)
WHERE 'SYM' IN r.brc
WITH DISTINCT type(r) AS relType

CALL {
  WITH relType
  MATCH (a)-[r1]->(b)
  WHERE type(r1)=relType AND 'SYM' IN r1.brc
    // counterexample: missing reverse edge b→a
    AND NOT EXISTS {
      MATCH (b)-[r2]->(a)
      WHERE type(r2)=relType AND 'SYM' IN r2.brc
    }
  RETURN COUNT(*) AS violations
}

RETURN relType        AS relation,
       violations = 0 AS isSymmetric;

/* TRA */
MATCH ()-[r]->()
WHERE 'TRA' IN r.brc
WITH DISTINCT type(r) AS relType, 'TRA' AS tag

CALL {
  WITH relType, tag
  MATCH (a)-[r1]->(b)-[r2]->(c)
  WHERE type(r1)=relType AND type(r2)=relType
    AND tag IN r1.brc AND tag IN r2.brc
    // counterexample: no direct edge a→c of the same type
    AND NOT EXISTS {
      MATCH (a)-[r3]->(c)
      WHERE type(r3)=relType AND tag IN r3.brc
    }
  RETURN COUNT(*) AS violations
}

RETURN relType        AS relation,
       violations = 0 AS isTransitive;

/* IRF */
MATCH ()-[r]->()
WHERE 'IRF' IN r.brc
WITH DISTINCT type(r) AS relType

CALL {
  WITH relType
  MATCH (n)-[r1]->(n)               // self-loop v→v
  WHERE type(r1)=relType AND 'IRF' IN r1.brc
  RETURN COUNT(r1) AS violations
}

RETURN relType        AS relation,
       violations = 0 AS isIrreflexive;

/* ASM */
MATCH ()-[r]->()
WHERE 'ASM' IN r.brc
WITH DISTINCT type(r) AS relType

CALL {
  WITH relType
  MATCH (a)-[r1]->(b)
  WHERE type(r1)=relType AND 'ASM' IN r1.brc
    // counterexample: a reverse edge b→a exists
    AND EXISTS {
      MATCH (b)-[r2]->(a)
      WHERE type(r2)=relType AND 'ASM' IN r2.brc
    }
  RETURN COUNT(r1) AS violations
}

RETURN relType        AS relation,
       violations = 0 AS isAsymmetric;

/* ANS */
MATCH ()-[r]->()
WHERE 'ANS' IN r.brc
WITH DISTINCT type(r) AS relType

CALL {
  WITH relType
  // OPTIONAL ensures a single row; LIMIT 1 cuts processing early.
  OPTIONAL MATCH (a)-[r1]->(b)
  WHERE type(r1) = relType AND 'ANS' IN r1.brc AND id(a) < id(b)
  OPTIONAL MATCH (b)-[r2]->(a)
  WHERE type(r2) = relType AND 'ANS' IN r2.brc
  WITH r2
  LIMIT 1
  RETURN r2 IS NOT NULL AS hasViolation
}

RETURN relType AS relation,
       NOT hasViolation AS isAntisymmetric;


/* ACC (acyclic) */
MATCH ()-[r]->()
WHERE 'ACC' IN r.brc
WITH DISTINCT type(r) AS relType

CALL {
  WITH relType
  // We look for any cycle; OPTIONAL guarantees a single row.
  OPTIONAL MATCH (a)-[r1]->(b)
  WHERE type(r1) = relType AND 'ACC' IN r1.brc
  OPTIONAL MATCH p = (b)-[rs*1..]->(a)
  WHERE ALL(r IN rs WHERE type(r) = relType AND 'ACC' IN r.brc)
  WITH p
  LIMIT 1
  RETURN p IS NOT NULL AS hasCycle
}

RETURN relType AS relation,
       NOT hasCycle AS isAcyclic;



/* ITR */
MATCH ()-[r]->()
WHERE 'ITR' IN r.brc
WITH DISTINCT type(r) AS relType, 'ITR' AS tag

CALL {
  WITH relType, tag
  // witness of intransitivity: a→b, b→c, but no a→c
  MATCH (a)-[r1]->(b)-[r2]->(c)
  WHERE type(r1)=relType AND type(r2)=relType
    AND tag IN r1.brc AND tag IN r2.brc
    AND NOT EXISTS {
      MATCH (a)-[r3]->(c)
      WHERE type(r3)=relType AND tag IN r3.brc
    }
  RETURN COUNT(*) AS witnesses
}

RETURN relType          AS relation,
       witnesses > 0    AS isIntransitive;

/* ANT */
MATCH ()-[r]->()
WHERE 'ANT' IN r.brc
WITH DISTINCT type(r) AS relType, 'ANT' AS tag

CALL {
  WITH relType, tag
  // counterexample: a→b, b→c and a→c exists
  MATCH (a)-[r1]->(b)-[r2]->(c)
  WHERE type(r1)=relType AND type(r2)=relType
    AND tag IN r1.brc AND tag IN r2.brc
  MATCH (a)-[r3]->(c)
  WHERE type(r3)=relType AND tag IN r3.brc
  RETURN COUNT(*) AS violations
}

RETURN relType        AS relation,
       violations = 0 AS isAntitransitive;

