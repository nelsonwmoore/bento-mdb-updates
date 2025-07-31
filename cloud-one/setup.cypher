create index if not exists for (n:node) on (n.nanoid)
create index if not exists for (n:relationship) on (n.nanoid)
create index if not exists for (n:property) on (n.nanoid)
create index if not exists for (n:value_set) on (n.nanoid)
create index if not exists for (n:term) on (n.nanoid)
create index if not exists for (n:origin) on (n.nanoid)
create index if not exists for (n:tag) on (n.key, n.value)
create fulltext index termDefn for (t:term) on each [t.origin_definition]
create fulltext index termValue for (t:term) on each [t.value]
create fulltext index termValueDefn for (t:term) on each [t.value, t.origin_defintion]
create fulltext index tagKeyValue for (g:tag) on each [g.key, g.value]
create fulltext index nodeHandle for (n:node) on each [n.handle]
create fulltext index edgeHandle for (n:relationship) on each [n.handle]
create fulltext index propHandle for (n:property) on each [n.handle]
CALL apoc.trigger.add('add-nanoid-property', ' UNWIND $createdNodes AS node WITH node WHERE ANY(label IN labels(node) WHERE label IN ["node", "relationship", "property", "value_set", "concept", "predicate", "term", "origin", "model", "tag"]) AND NOT exists(node.nanoid) SET node.nanoid = apoc.text.random(6, "abcdefghijkmnopqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ0123456789") ', { phase: "afterAsync" })
CREATE CONSTRAINT node_unique_attrs IF NOT EXISTS FOR (n:node) REQUIRE (n.model, n.handle) IS UNIQUE
