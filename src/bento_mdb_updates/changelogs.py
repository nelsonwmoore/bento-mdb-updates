"""Common functions shared by changelog generation scripts."""

from __future__ import annotations

import logging
from datetime import datetime
from string import Template
from typing import TYPE_CHECKING

from bento_meta.mdb.mdb import make_nanoid
from bento_meta.objects import Concept, Edge, Property, Tag, Term, ValueSet
from bento_meta.objects import Model as ModelEnt
from liquichange.changelog import Changelog, Changeset, CypherChange, Rollback
from minicypher.clauses import (
    Clause,
    Create,
    Match,
    Merge,
    OnCreateSet,
    OptionalMatch,
    Where,
)
from minicypher.entities import G, N, P, R, T, _condition, _return
from minicypher.functions import Func
from tqdm import tqdm

if TYPE_CHECKING:
    from bento_meta.entity import Entity
    from bento_meta.model import Model

    from bento_mdb_updates.datatypes import AnnotationSpec, ModelCDESpec

logger = logging.getLogger(__name__)
DEFAULT_COMMIT = f"CDEPV-{datetime.today().strftime('%Y%m%d')}"
DEFAULT_AUTHOR = "DEFAULT"


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
    _commit: str | None = DEFAULT_COMMIT,
) -> list[Changeset]:
    """Convert annotation to list of Liquibase Changesets."""
    if not annotation.get("value_set") or annotation.get("value_set") == []:
        return []
    statements: list[Statement] = []
    changesets = []
    cde_attrs = annotation["annotation"]["attrs"]
    base_url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"
    cde_id = cde_attrs.get("origin_id", "")
    cde_ver = cde_attrs.get("origin_version", "")
    if cde_ver is None:
        cde_ver = ""
    cde_vs = ValueSet(
        {
            "url": f"{base_url}{cde_id}{f'?version={cde_ver}' if cde_ver else ''}",
            "handle": f"{cde_id}|{cde_ver}",
            "_commit": _commit,
        },
    )
    statements.append(create_entity_cypher_stmt(cde_vs)[0])
    for pv in tqdm(
        annotation["value_set"],
        desc="PVs",
        total=len(annotation["value_set"]),
    ):
        if not pv:
            continue
        synonyms = pv.pop("synonyms")  # separate synonyms dict from pv attrs
        pv_term = Term(pv)
        pv_term._commit = _commit  # noqa: SLF001
        statements.append(create_entity_cypher_stmt(pv_term)[0])
        statements.append(
            create_relationship_cypher_stmt(cde_vs, "has_term", pv_term)[0],
        )
        if not synonyms:
            continue
        for syn_attrs in synonyms:
            syn_term = Term(syn_attrs)
            mapping_source = "caDSR" if syn_term.origin_name == "NCIt" else "NCIm"
            statements.append(create_entity_cypher_stmt(syn_term)[0])
            statements.append(
                generate_cypher_to_link_term_synonyms(
                    pv_term,
                    syn_term,
                    mapping_source,
                    _commit,
                ),
            )

    # create changesets for each statement
    cs_id = changeset_id
    for stmt in statements:
        str_stmt = str(stmt).replace("\\'", "'")
        changesets.append(
            Changeset(
                id=str(cs_id),
                author=author,
                change_type=CypherChange(text=str_stmt),
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
    if not author:
        author = DEFAULT_AUTHOR
    if not _commit:
        _commit = DEFAULT_COMMIT
    for annotation in tqdm(model_cdes["annotations"], desc="Annotations"):
        msg = f"Annotation: {annotation['entity'].get('key', '')}"
        logging.info(msg)
        changesets = convert_annotation_to_changesets(
            annotation,
            changeset_id,
            author,
            _commit,
        )
        if not changesets:
            continue
        changeset_id += len(changesets)
        for changeset in tqdm(changesets, desc="Changesets", total=len(changesets)):
            changelog.add_changeset(changeset)
        del changesets  # garbage collection
    return changelog


def add_version_to_model_ents(model: Model) -> None:
    """Set model version for entities in model."""
    for node in model.nodes.values():
        if node.version:
            continue
        node.version = model.version
    for edge in model.edges.values():
        if edge.version:
            continue
        edge.version = model.version
    for prop in model.props.values():
        if prop.version:
            continue
        prop.version = model.version


def separate_shared_props(model: Model) -> None:
    """
    Duplicate properties shared by > 1 entity with new nanoid.

    This ensures each entity has its own copy.
    """
    initial_props = set()

    for key, prop in model.props.items():
        if prop in initial_props:
            new_prop = prop.dup()
            if new_prop.nanoid:
                new_prop.nanoid = make_nanoid()
            if prop._commit:
                new_prop._commit = prop._commit
            if prop.value_set:
                new_prop.value_set = prop.value_set.dup()
            model.nodes[key[0]].props[key[1]] = new_prop
            model.props[(key[0], key[1])] = new_prop
        else:
            initial_props.add(prop)


class ModelToChangelogConverter:
    """Class to convert bento-meta model object to a liquibase changelog."""

    def __init__(
        self,
        model: Model,
        *,
        add_rollback: bool = True,
        terms_only: bool = False,
    ) -> None:
        """Initialize converter and structures to hold cypher stmts & added entities."""
        self.add_rollback = add_rollback
        self.terms_only = terms_only
        self.model = model
        self.cypher_stmts: dict[str, dict[str, list[Statement]]] = {
            "add_ents": {"statements": [], "rollbacks": []},
            "add_rels": {"statements": [], "rollbacks": []},
        }
        self.added_entities = []

    def add_statement(
        self,
        stmt_type: str,
        stmt: Statement,
        rollback: Statement,
    ) -> None:
        """Add cypher statement and its rollback to self.cypher_stmts."""
        self.cypher_stmts[stmt_type]["statements"].append(stmt)
        self.cypher_stmts[stmt_type]["rollbacks"].append(rollback)

    def generate_cypher_to_add_entity(
        self,
        entity: Entity,
    ) -> None:
        """Generate cypher statement to create or merge Entity."""
        stmt_type = "add_ents"
        if entity in self.added_entities:
            msg = f"Entity with attrs: {entity.get_attr_dict()} already added."
            logger.info(msg)
            return
        stmt, rollback = create_entity_cypher_stmt(entity)
        self.add_statement(stmt_type, stmt, rollback)
        self.added_entities.append(entity)

    def generate_cypher_to_add_relationship(
        self,
        src: Entity,
        rel: str,
        dst: Entity,
    ) -> None:
        """Generate cypher statement to create relationship from src to dst entity."""
        stmt_type = "add_rels"
        stmt, rollback = create_relationship_cypher_stmt(src, rel, dst)
        self.add_statement(stmt_type, stmt, rollback)

    def process_tags(self, entity: Entity) -> None:
        """Generate cypher statements to create/merge an entity's tag attributes."""
        if not entity.tags:
            return
        for tag in entity.tags.values():
            if not tag.nanoid:
                tag.nanoid = make_nanoid()
            if not tag._parent:
                tag._parent = entity
            self.generate_cypher_to_add_entity(tag)
            self.generate_cypher_to_add_relationship(entity, "has_tag", tag)

    def process_origin(self, entity: Entity) -> None:
        """Generate cypher statements to create/merge an entity's origin attribute."""
        if not entity.origin:
            return
        self.generate_cypher_to_add_entity(entity.origin)
        self.generate_cypher_to_add_relationship(entity, "has_origin", entity.origin)
        self.process_tags(entity.origin)

    def process_terms(self, entity: Entity) -> None:
        """Generate cypher statements to create/merge an entity's term attributes."""
        if not entity.terms:
            return
        for term in entity.terms.values():
            self.generate_cypher_to_add_entity(term)
            if isinstance(entity, Concept):
                self.generate_cypher_to_add_relationship(term, "represents", entity)
            else:
                self.generate_cypher_to_add_relationship(entity, "has_term", term)
            self.process_tags(term)
            self.process_origin(term)
            self.process_concept(term)

    def process_concept(self, entity: Entity) -> None:
        """Generate cypher statements to create/merge an entity's concept attribute."""
        if not entity.concept:
            return
        if not entity.concept.tags.get("mapping_source"):
            entity.concept.tags["mapping_source"] = Tag(
                {"key": "mapping_source", "value": self.model.handle},
            )
        self.generate_cypher_to_add_entity(entity.concept)
        self.generate_cypher_to_add_relationship(entity, "has_concept", entity.concept)
        self.process_tags(entity.concept)
        self.process_terms(entity.concept)

    def process_value_set(self, entity: Entity) -> None:
        """Generate cypher statements to create/merge an entity's value_set attribute."""
        if not entity.value_set:
            return
        if not entity.value_set.nanoid:
            entity.value_set.nanoid = make_nanoid()
        self.generate_cypher_to_add_entity(entity.value_set)
        self.generate_cypher_to_add_relationship(
            entity,
            "has_value_set",
            entity.value_set,
        )
        self.process_tags(entity.value_set)
        self.process_origin(entity.value_set)
        self.process_terms(entity.value_set)

    def process_props(self, entity: Entity) -> None:
        """Generate cypher statements to create/merge an entity's props attribute."""
        if not entity.props:
            return
        for prop in entity.props.values():
            if not prop.nanoid:
                prop.nanoid = make_nanoid()
            if not prop._parent_handle:
                prop._parent_handle = entity.handle
            self.generate_cypher_to_add_entity(prop)
            self.generate_cypher_to_add_relationship(entity, "has_property", prop)
            self.process_tags(prop)
            self.process_concept(prop)
            self.process_value_set(prop)

    def process_model_nodes(self) -> None:
        """Generate cypher statements to create/merge an model's nodes."""
        for node in self.model.nodes.values():
            self.generate_cypher_to_add_entity(node)
            self.process_tags(node)
            self.process_concept(node)
            self.process_props(node)

    def process_model_edges(self) -> None:
        """Generate cypher statements to create/merge an model's edges."""
        for edge in self.model.edges.values():
            if not edge.nanoid:
                edge.nanoid = make_nanoid()
            self.generate_cypher_to_add_entity(edge)
            self.generate_cypher_to_add_relationship(edge, "has_src", edge.src)
            self.generate_cypher_to_add_relationship(edge, "has_dst", edge.dst)
            self.process_tags(edge)
            self.process_concept(edge)
            self.process_props(edge)

    def process_terms_model(self) -> None:
        """
        Generate cypher statements to create/merge a model's terms.

        Used for large term sets w/o model structure.
        Does not process placeholder node/relationship/props.
        """
        logger.info("Processing terms-only model.")
        prop_terms = [p.terms for p in self.model.props.values()]
        flat_prop_terms = [v for d in prop_terms for v in d.values()]
        for term in flat_prop_terms + list(self.model.terms.values()):
            self.generate_cypher_to_add_entity(term)
            self.process_tags(term)
            self.process_origin(term)
            self.process_concept(term)

    def set_model_version(
        self,
        *,
        latest_version: bool = False,
    ) -> None:
        """Create model node and set model version for other entities."""
        model_ent = ModelEnt(
            {
                "latest_version": latest_version,
                "repository": self.model.uri,
                "handle": self.model.handle,
                "version": self.model.version,
                "name": self.model.handle,
            },
        )
        self.generate_cypher_to_add_entity(model_ent)
        if model_ent.version is not None:
            add_version_to_model_ents(self.model)

    def convert_model_to_changelog(
        self,
        author: str,
        model_version: str | None = None,
        *,
        latest_version: bool = False,
    ) -> Changelog:
        """
        Convert a bento meta model to a Liquibase Changelog.

        Parameters:
        model (Model): The bento meta model to convert
        author (str): The author for the changesets

        Returns:
        Changelog: The generated Liquibase Changelog

        Functionality:
        - Generates Cypher statements to add entities and relationships from the model
        - Appends Changesets with the Cypher statements to a Changelog
        - Returns the completed Changelog
        """
        # if property shared by multiple nodes/edges,
        separate_shared_props(self.model)

        # create model node and add model version to other entities
        if model_version is not None:
            self.model.version = model_version
        self.set_model_version(latest_version=latest_version)

        if not self.terms_only:
            self.process_model_nodes()
            self.process_model_edges()
        else:
            self.process_terms_model()

        changeset_id = 1
        changelog = Changelog()

        for stmts in self.cypher_stmts.values():
            for stmt, rollback in zip(stmts["statements"], stmts["rollbacks"]):
                # testing replacing poorly escaped quotes
                str_stmt = str(stmt).replace("\\'", "'")
                changeset = Changeset(
                    id=str(changeset_id),
                    author=author,
                    change_type=CypherChange(text=str_stmt),
                )
                if self.add_rollback:
                    changeset.set_rollback(Rollback(text=str(rollback)))
                changelog.add_changeset(changeset)
                changeset_id += 1

        return changelog


def create_entity_cypher_stmt(
    entity: Entity,
) -> tuple[Statement, Statement]:
    """Generate cypher statement to create or merge Entity."""
    escape_quotes_in_attr(entity)
    reset_pg_ent_counter()
    cypher_ent = cypherize_entity(entity)
    if isinstance(entity, Property) and "_parent_handle" in cypher_ent.props:
        cypher_ent.props.pop("_parent_handle")
    if isinstance(entity, (Term, ValueSet, Concept)):
        if "_commit" not in cypher_ent.props:
            stmt = Statement(Merge(cypher_ent))
        # remove _commit prop of Term/VS cypher_ent for Merge
        else:
            commit = cypher_ent.props.pop("_commit", DEFAULT_COMMIT)
            stmt = Statement(Merge(cypher_ent), OnCreateSet(commit))
        rollback = Statement("empty")
    else:
        stmt = Statement(Create(cypher_ent))
        rollback = Statement(
            Match(cypher_ent),
            DetachDelete(cypher_ent.plain_var()),
        )
    return stmt, rollback


def create_relationship_cypher_stmt(
    src: Entity,
    rel: str,
    dst: Entity,
) -> tuple[Statement, Statement]:
    """Generate cypher statement to create relationship from src to dst entity."""
    reset_pg_ent_counter()
    cypher_src = cypherize_entity(src)
    cypher_dst = cypherize_entity(dst)
    cypher_rel = R(Type=rel)
    # remove _commit attr from Term and VS ents
    for cypher_ent in (cypher_src, cypher_dst):
        if cypher_ent.label in ("term", "value_set") and "_commit" in cypher_ent.props:
            cypher_ent.props.pop("_commit", DEFAULT_COMMIT)
        if cypher_ent.label == "property" and "_parent_handle" in cypher_ent.props:
            cypher_ent.props.pop("_parent_handle")
    stmt_merge_trip = T(cypher_src.plain_var(), cypher_rel, cypher_dst.plain_var())
    rlbk_match_trip = T(cypher_src, cypher_rel, cypher_dst)
    return (
        Statement(Match(cypher_src, cypher_dst), Merge(stmt_merge_trip)),
        Statement(Match(rlbk_match_trip), Delete(cypher_rel.plain_var())),
    )


def generate_cypher_to_link_term_synonyms(
    entity_1: Entity,
    entity_2: Entity,
    mapping_source: str,
    _commit: str | None = DEFAULT_COMMIT,
) -> Statement:
    """
    Generate cypher statement to link two terms via a Concept node.

    Finds or creates one Concept node and ensures both Terms connected to it via the
    'represents' relationship. If either Term is already connected to a Concept tagged
    by the given mapping source, that concept is used instead.
    """
    reset_pg_ent_counter()
    cypher_ent_1 = cypherize_entity(entity_1)
    cypher_ent_2 = cypherize_entity(entity_2)
    cypher_concept_1 = N(label="concept")
    cypher_concept_2 = N(label="concept")
    ent_1_trip = T(cypher_ent_1.plain_var(), R(Type="represents"), cypher_concept_1)
    ent_2_trip = T(cypher_ent_2.plain_var(), R(Type="represents"), cypher_concept_2)
    concept_tag_trip_1 = T(
        cypher_concept_1,
        R(Type="has_tag"),
        N(
            label="tag",
            props={"key": "mapping_source", "value": mapping_source},
        ),
    )
    concept_tag_trip_2 = T(
        cypher_concept_2,
        R(Type="has_tag"),
        N(
            label="tag",
            props={"key": "mapping_source", "value": mapping_source},
        ),
    )
    ent_1_concept_path = G(ent_1_trip, concept_tag_trip_1)
    ent_2_concept_path = G(ent_2_trip, concept_tag_trip_2)
    cypher_ent_1_var = cypher_ent_1.plain_var().pattern()
    cypher_ent_2_var = cypher_ent_2.plain_var().pattern()
    cypher_concept_1_var = cypher_concept_1.plain_var().pattern()
    cypher_concept_2_var = cypher_concept_2.plain_var().pattern()
    new_concept = N(label="concept", props={"_commit": _commit})
    for cypher_ent in (cypher_ent_1, cypher_ent_2):
        if "_commit" in cypher_ent.props:
            cypher_ent.props.pop("_commit", DEFAULT_COMMIT)
    return Statement(
        Match(cypher_ent_1, cypher_ent_2),
        Where(cypher_ent_1_var, "<>", cypher_ent_2_var, op=""),
        With(cypher_ent_1_var, cypher_ent_2_var),
        OptionalMatch(ent_1_concept_path),
        With(cypher_ent_1_var, cypher_ent_2_var, cypher_concept_1_var),
        "LIMIT 1",
        OptionalMatch(ent_2_concept_path),
        With(
            cypher_ent_1_var,
            cypher_ent_2_var,
            cypher_concept_1_var,
            cypher_concept_2_var,
        ),
        "LIMIT 1",
        With(cypher_ent_1_var, cypher_ent_2_var),
        ",",
        f"{Case()}{When(cypher_concept_1_var)} IS NOT NULL THEN {cypher_concept_1_var}",
        f"{When(cypher_concept_2_var)} IS NOT NULL THEN {cypher_concept_2_var}",
        "ELSE NULL END AS existing_concept ",
        ForEach(),
        f"(_ IN {Case()}{When('existing_concept')} IS NOT NULL THEN [1] ELSE [] END |",
        Merge(f"{cypher_ent_1_var}-[:represents]->(existing_concept)"),
        Merge(f"{cypher_ent_2_var}-[:represents]->(existing_concept)"),
        ")",
        ForEach(),
        f"(_ IN {Case()}{When('existing_concept')} IS NULL THEN [1] ELSE [] END |",
        Create(new_concept),
        Create(
            T(
                new_concept.plain_var(),
                R("has_tag"),
                N(
                    label="tag",
                    props={"key": "mapping_source", "value": mapping_source},
                ),
            ),
        ),
        Create(T(cypher_ent_1.plain_var(), R("represents"), new_concept.plain_var())),
        Create(T(cypher_ent_2.plain_var(), R("represents"), new_concept.plain_var())),
        ")",
    )
