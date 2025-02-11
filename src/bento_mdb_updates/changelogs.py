"""Common functions shared by changelog generation scripts."""

from __future__ import annotations

import logging
from string import Template
from typing import TYPE_CHECKING

from bento_meta.mdb.mdb import make_nanoid
from bento_meta.objects import Concept, Edge, Property, Tag, Term, ValueSet
from liquichange.changelog import Changelog, Changeset, CypherChange
from minicypher.clauses import Clause, Match, Merge, OnCreateSet
from minicypher.entities import G, N, P, R, T, _condition, _return
from minicypher.functions import Func
from tqdm import tqdm

if TYPE_CHECKING:
    from bento_meta.entity import Entity

    from bento_mdb_updates.datatypes import AnnotationSpec, ModelCDESpec

logger = logging.getLogger(__name__)


def cypherize_entity(entity: Entity) -> N:
    """Represent metamodel Entity object as a property graph Node."""
    return N(label=entity.get_label(), props=entity.get_attr_dict())


def escape_quotes_in_attr(entity: Entity) -> None:
    """
    Escapes quotes in entity attributes.

    Quotes in string attributes may or may not already be escaped, so this function
    unescapes all previously escaped ' and " characters and replaces them with
    """
    for attr in entity.attspec:
        val = getattr(entity, attr, None)
        if val is not None and isinstance(val, str):
            # First unescape any previously escaped quotes
            unescape_val = val.replace(r"\'", "'").replace(r"\"", '"')

            # Escape all quotes, use utf-8 encoded versions of backslash and quotes
            # so extra backslash isn't added to the string
            escape_val = unescape_val.replace(
                r"'",
                "\u005c\u0027",
            ).replace(
                r'"',
                "\u005c\u0022",
            )

            setattr(entity, attr, escape_val)


def reset_pg_ent_counter() -> None:
    """Reset property graph entity variable counters to 0."""
    N._reset_counter()
    R._reset_counter()


def generate_match_clause(entity: Entity, ent_c: N) -> Match:
    """Generate Match clause for entity."""
    if isinstance(entity, Edge):
        return match_edge(edge=entity, ent_c=ent_c)
    if isinstance(entity, Property):  # remove '_parent_handle' from ent_c property
        ent_c.props.pop("_parent_handle", None)
        return match_prop(prop=entity, ent_c=ent_c)
    if isinstance(entity, Tag):
        return match_tag(tag=entity, ent_c=ent_c)
    return Match(ent_c)


def match_edge(edge: Edge, ent_c: N) -> Match:
    """Add MATCH statement for edge."""
    src_c = N(label="node", props=edge.src.get_attr_dict())
    dst_c = N(label="node", props=edge.dst.get_attr_dict())
    src_trip = T(ent_c, R(Type="has_src"), src_c)
    dst_trip = T(ent_c, R(Type="has_dst"), dst_c)
    path = G(src_trip, dst_trip)
    return Match(path)


def match_prop(prop: Property, ent_c: N) -> Match:
    """Add MATCH statement for property."""
    if not prop._parent_handle:
        msg = f"Property missing parent handle {prop.get_attr_dict()}"
        raise AttributeError(msg)
    par_c = N(props={"handle": prop._parent_handle})
    prop_trip = T(par_c, R(Type="has_property"), ent_c)
    return Match(prop_trip)


def match_tag(tag: Tag, ent_c: N) -> Match:
    """Add MATCH statement for tag."""
    if not tag._parent:
        msg = f"Tag missing parent {tag.get_attr_dict()}"
        raise AttributeError(msg)
    parent = tag._parent
    par_c = N(label=parent.get_label(), props=parent.get_attr_dict())
    par_c.props.pop("_parent_handle", None)
    # temp workaround for long matches
    par_match_clause = generate_match_clause(entity=parent, ent_c=par_c)
    par_match_str = str(par_match_clause)[6:]
    tag_trip = T(par_c.plain_var(), R(Type="has_tag"), ent_c)
    return Match(par_match_str, tag_trip)


class Case(Clause):
    """Create a CASE clause with the arguments."""

    template = Template("CASE $slot1")

    def __init__(self, *args):
        super().__init__(*args)


class Delete(Clause):
    """Create a DELETE clause with the arguments."""

    template = Template("DELETE $slot1")

    def __init__(self, *args):
        super().__init__(*args)


class DetachDelete(Clause):
    """Create a DETACH DELETE clause with the arguments."""

    template = Template("DETACH DELETE $slot1")

    def __init__(self, *args):
        super().__init__(*args)


class ForEach(Clause):
    """Create an FOREACH clause with the arguments."""

    template = Template("FOREACH $slot1")

    def __init__(self, *args):
        super().__init__(*args)


class Statement:
    """Create a Neo4j statement comprised of clauses (and strings) in order."""

    def __init__(self, *args, terminate=False, use_params=False):
        self.clauses = args
        self.terminate = terminate
        self.use_params = use_params
        self._params = None

    def __str__(self):
        stash = P.parameterize
        if self.use_params:
            P.parameterize = True
        else:
            P.parameterize = False
        ret = " ".join([str(x) for x in self.clauses])
        if self.terminate:
            ret = ret + ";"
        P.parameterize = stash
        return ret

    @property
    def params(self):
        if self._params is None:
            self._params = {}
            for c in self.clauses:
                for ent in c.args:
                    if isinstance(ent, (N, R)):
                        for p in ent.props.values():
                            self._params[p.var] = p.value
                    else:
                        if "nodes" in vars(type(ent)):
                            for n in ent.nodes():
                                for p in n.props.values():
                                    self._params[p.var] = p.value
                        if "edges" in vars(type(ent)):
                            for e in ent.edges():
                                for p in e.props.values():
                                    self._params[p.var] = p.value
        return self._params


class With(Clause):
    """Create a WITH clause with the arguments."""

    template = Template("WITH $slot1")

    def __init__(self, *args):
        super().__init__(*args)

    @staticmethod
    def context(arg: object) -> str:
        return _return(arg)


class When(Clause):
    """Create a WHEN clause with the arguments."""

    template = Template("WHEN $slot1")
    joiner = " {} "

    @staticmethod
    def context(arg):
        return _condition(arg)

    def __init__(self, *args, op="AND"):
        super().__init__(*args, op=op)
        self.op = op

    def __str__(self):
        values = []
        for c in [self.context(x) for x in self.args]:
            if isinstance(c, str):
                values.append(c)
            elif isinstance(c, Func):
                values.append(str(c))
            elif isinstance(c, list):
                values.extend([str(x) for x in c])
            else:
                values.append(str(c))
        return self.template.substitute(slot1=self.joiner.format(self.op).join(values))


def convert_annotation_to_changesets(
    annotation: AnnotationSpec,
    changeset_id: int,
    author: str | None = None,
    _commit: str | None = None,
) -> list[Changeset]:
    """Convert annotation to list of Liquibase Changesets."""
    if (
        annotation["entity"]["attrs"]["value_domain"] != "value_set"
        and not annotation["entity"]["entity_has_enum"]
    ):
        return []
    statements = []
    changesets = []
    cde_attrs = annotation["annotation"]["attrs"]
    base_url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"
    cde_id = cde_attrs.get("origin_id", "")
    cde_ver = cde_attrs.get("origin_version", "")
    cde_vs = ValueSet(
        {
            "url": f"{base_url}{cde_id}{f'?version={cde_ver}' if cde_ver else ''}",
            "handle": f"{cde_id}|{cde_ver}",
            "_commit": _commit,
        },
    )
    cypher_cde_vs = cypherize_entity(cde_vs)
    vs_commit = cypher_cde_vs.props.pop("_commit")
    statements.append(Statement(Merge(cypher_cde_vs), OnCreateSet(vs_commit)))
    # load entities from annotation
    # need terms for each pv in value set and synonyms for each of those pvs
    for pv in tqdm(
        annotation["value_set"],
        desc="PVs",
        total=len(annotation["value_set"]),
    ):
        synonyms = pv.pop("synonyms")  # separate synonyms dict from pv attrs
        pv_term = Term(pv)
        pv_term._commit = _commit  # noqa: SLF001
        escape_quotes_in_attr(pv_term)
        cypher_pv_term = cypherize_entity(pv_term)
        term_commit = cypher_pv_term.props.pop("_commit")
        statements.append(Statement(Merge(cypher_pv_term), OnCreateSet(term_commit)))

        # add relationship between cde value set & pv term(s)
        cypher_rel = R(Type="has_term")
        stmt_merge_trip = T(
            cypher_cde_vs.plain_var(),
            cypher_rel,
            cypher_pv_term.plain_var(),
        )
        statements.append(
            Statement(Match(cypher_cde_vs, cypher_pv_term), Merge(stmt_merge_trip)),
        )
        # make concept for pv & its synonyms
        concept = Concept({"_commit": _commit, "nanoid": make_nanoid()})
        cypher_concept = cypherize_entity(concept)
        concept_commit = cypher_concept.props.pop("_commit")

        # create statements for synonyms
        if synonyms:
            # add statement to create concept and link pv term to concept
            statements.append(
                Statement(Merge(cypher_concept), OnCreateSet(concept_commit)),
            )
            pv_concept_cypher_rel = R(Type="represents")
            pv_stmt_merge_trip = T(
                cypher_pv_term.plain_var(),
                pv_concept_cypher_rel,
                cypher_concept.plain_var(),
            )
            statements.append(
                Statement(
                    Match(cypher_pv_term, cypher_concept),
                    Merge(pv_stmt_merge_trip),
                ),
            )
            for syn_attrs in synonyms:
                syn_term = Term(syn_attrs)
                syn_term._commit = _commit  # noqa: SLF001
                escape_quotes_in_attr(syn_term)
                cypher_syn_term = cypherize_entity(syn_term)
                syn_commit = cypher_syn_term.props.pop("_commit")
                # add synonym term
                statements.append(
                    Statement(Merge(cypher_syn_term), OnCreateSet(syn_commit)),
                )
                # link synonym term to pv term via concept
                syn_cypher_rel = R(Type="represents")
                syn_stmt_merge_trip = T(
                    cypher_syn_term.plain_var(),
                    syn_cypher_rel,
                    cypher_concept.plain_var(),
                )
                statements.append(
                    Statement(
                        Match(cypher_syn_term, cypher_concept),
                        Merge(syn_stmt_merge_trip),
                    ),
                )

    # create changesets for each statement
    cs_id = changeset_id
    for stmt in statements:
        changesets.append(
            Changeset(
                id=str(cs_id),
                author=author,
                change_type=CypherChange(text=str(stmt)),
            ),
        )
        cs_id += 1

    del statements  # garbage collection
    return changesets


def convert_model_cdes_to_changelog(
    model_cdes: ModelCDESpec,
    author: str | None = None,
    _commit: str | None = None,
) -> Changelog:
    """Convert model cde annotations with PVs and synonyms to Liquibase Changelog."""
    changelog = Changelog()
    changeset_id = 1
    for annotation in tqdm(model_cdes["annotations"], desc="Annotations"):
        msg = f"Annotation: {annotation['entity']['key']}"
        logging.info(msg)
        changesets = convert_annotation_to_changesets(
            annotation,
            changeset_id,
            author,
            _commit,
        )
        if not changesets:
            continue
        for changeset in tqdm(changesets, desc="Changesets", total=len(changesets)):
            changelog.add_changeset(changeset)
        del changesets  # garbage collection
    return changelog
