package no.bouvet.genealogy;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.corba.se.spi.activation._RepositoryImplBase;
import org.gedcom4j.model.*;
import org.gedcom4j.parser.GedcomParser;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.Iterables.isEmpty;
import static java.util.stream.Collectors.toList;

public class GedcomToNeo4J {

    private static final Logger LOG = LoggerFactory.getLogger(GedcomToNeo4J.class);

    private final Label LBL_PERSON = DynamicLabel.label("Person");
    private final Label LBL_FAMILY = DynamicLabel.label("Familie");
    private final Label LBL_HENDELSE = DynamicLabel.label("Hendelse");
    private final Label LBL_STED = DynamicLabel.label("Sted");
    private final Label LBL_KILDE = DynamicLabel.label("Kilde");

    private static Map<String, String> HENDELSE_TYPE_MAPPING = Maps.newHashMap();

    static {
        HENDELSE_TYPE_MAPPING.put("EVEN", "Hendelse");
        HENDELSE_TYPE_MAPPING.put("BIRT", "Fødsel");
        HENDELSE_TYPE_MAPPING.put("DEAT", "Død");
        HENDELSE_TYPE_MAPPING.put("OCCU", "Yrke");
        HENDELSE_TYPE_MAPPING.put("RESI", "Bosted");
        HENDELSE_TYPE_MAPPING.put("BAPM", "Dåp");
        HENDELSE_TYPE_MAPPING.put("ADOP", "Adopsjon");
        HENDELSE_TYPE_MAPPING.put("CENS", "Folketelling");
        HENDELSE_TYPE_MAPPING.put("MARR", "Ekteskap");
        HENDELSE_TYPE_MAPPING.put("BURI", "Begravelse");
        HENDELSE_TYPE_MAPPING.put("PROB", "Skifte");
        HENDELSE_TYPE_MAPPING.put("CONF", "Konfirmasjon");
        HENDELSE_TYPE_MAPPING.put("ENGA", "Forlovelse");
        HENDELSE_TYPE_MAPPING.put("NATI", "Nasjonalitet");
        HENDELSE_TYPE_MAPPING.put("IMMI", "Immigrasjon");
        HENDELSE_TYPE_MAPPING.put("NATU", "Statsborgerskap");
        HENDELSE_TYPE_MAPPING.put("DIV", "Skilsmisse");
        HENDELSE_TYPE_MAPPING.put("DIVF", "Separasjon");
        HENDELSE_TYPE_MAPPING.put("RELI", "Religion");
        HENDELSE_TYPE_MAPPING.put("RETI", "Pensjon");
    }

        private GraphDatabaseService graphDb;

    public void load(String gedcomFilename, String databaseName) throws Exception {
        LOG.info("load('{}', '{}'", gedcomFilename, databaseName);

        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(databaseName);
        registerShutdownHook(graphDb);

        GedcomParser parser = new GedcomParser();
        parser.load(gedcomFilename);

        try (Transaction tx = graphDb.beginTx()) {
            parser.gedcom.families.values().forEach(f -> {
                Node family = createFamily(f);

                Node mother = createFamilyRelationship(family, f.wife, FamilieRelasjoner.HUSTRU);
                Node father = createFamilyRelationship(family, f.husband, FamilieRelasjoner.EKTEMANN);
                f.children.forEach(c -> {
                    Node child = createFamilyRelationship(family, c, FamilieRelasjoner.BARN);
                    createParentRelationship(child, mother, PersonRelasjoner.MOR, makeId(f.xref));
                    createParentRelationship(child, father, PersonRelasjoner.FAR, makeId(f.xref));
                });
                f.events.forEach(e -> family.createRelationshipTo(createEvent(e, e.type.tag), FamilieRelasjoner.HENDELSE));
            });
            tx.success();
        }
    }

    private void createParentRelationship(Node child, Node parent, RelationshipType relation, String familyRef) {
        if (parent != null) {
            LOG.info("createParentRelationship(({})-[:{}]->({})): Family {}",
                    new Object[]{child.getProperty("id"), relation.name(), parent.getProperty("id"), familyRef});
            child.createRelationshipTo(parent, relation).setProperty("familie", familyRef);
        }
    }

    private Node createFamilyRelationship(Node family, Individual member, RelationshipType relation) {
        Node person = null;
        if (member != null) {
            LOG.info("createFamilyRelationship(({})-[:{}]->({}))", family.getProperty("id"), relation.name(), makeId(member.xref));
            person = fetchOrCreateIndividual(member);
            family.createRelationshipTo(person, relation);
        }
        return person;
    }

    private Node createFamily(Family f) {
        LOG.info("createFamily('{}')", makeId(f.xref));
        Node family = graphDb.createNode(LBL_FAMILY);
        family.setProperty("id", makeId(f.xref));
        return family;
    }

    private Node fetchOrCreateIndividual(Individual individual) {
        LOG.info("fetchOrCreateIndividual('{}')", makeId(individual.xref));
        return fetchOrCreateAndPopulate(LBL_PERSON, "id", makeId(individual.xref), individual, (node, from) -> {
            node.setProperty("navn", mapToStringArray(individual.names, n -> n.basic.trim()));

            if (individual.sex != null) {
                node.setProperty("kjonn", individual.sex.value);
            }
            addNotes(node, individual.notes);
            addCitations(PersonRelasjoner.SITAT, individual.citations, node);

            individual.attributes.forEach(a -> node.createRelationshipTo(createEvent(a, a.type.tag), PersonRelasjoner.HENDELSE));
            individual.events.forEach(e -> node.createRelationshipTo(createEvent(e, e.type.tag), PersonRelasjoner.HENDELSE));
            individual.names.forEach(n -> addCitations(PersonRelasjoner.NAVNESITAT, n.citations, node));
        });
    }

    private void addNotes(Node individual, List<Note> notes) {
        if (!isEmpty(notes)) {
            individual.setProperty("notater", mapToStringArray(notes, str -> Joiner.on(' ').join(str.lines)));
        }
    }

    private Node createEvent(Event e, String type) {
        Node attributt = graphDb.createNode(LBL_HENDELSE);
        attributt.setProperty("type", mapHendelseType(type));

        if (e.date != null) {
            attributt.setProperty("dato", e.date.value);
        }
        if (e.place != null) {
            attributt.createRelationshipTo(fetchOrCreatePlace(e.place), HendelseRelasjoner.STED);
        }
        if (e.description != null && e.description.value != null) {
            attributt.setProperty("beskrivelse", e.description.value);
        }
        addNotes(attributt, e.notes);
        addCitations(HendelseRelasjoner.SITAT, e.citations, attributt);

        return attributt;
    }

    private void addCitations(RelationshipType type, List<AbstractCitation> citations, Node node) {
        if (citations != null) {
            citations.forEach(c -> createCitation(node, type, (CitationWithSource) c));
        }
    }

    private Node fetchOrCreatePlace(Place place) {
        LOG.info("fetchOrCreatePlace('{}')", place.placeName);
        return fetchOrCreatePlaceChain(Lists.newArrayList(Splitter.on(", ").split(place.placeName)));
    }

    private Node fetchOrCreatePlaceChain(List<String> places) {
        LOG.info("fetchOrCreatePlaceChain({})", places);

        List<Node> candidates = Lists.newArrayList(graphDb.findNodesByLabelAndProperty(LBL_STED, "navn", places.get(0)));
        LOG.trace("candidates: {}", candidates);
        Node node = candidates.stream().filter(candidate -> {
            List<Node> result = Lists.newArrayList(graphDb.traversalDescription()
                    .relationships(StedRelasjoner.PLASSERING, Direction.OUTGOING)
                    .traverse(candidate)
                    .nodes());

            return result.stream().map(place -> place.getProperty("navn")).collect(toList()).equals(places);
        }).findAny().orElse(null);

        if (node != null) {
            LOG.debug("Found existing place '{}'", places);
            return node;
        } else {
            LOG.debug("Creating new place '{}'", places.get(0));

            Node place = graphDb.createNode(LBL_STED);
            place.setProperty("navn", places.get(0));
            if (places.size() > 1) {
                place.createRelationshipTo(fetchOrCreatePlaceChain(places.subList(1, places.size())), StedRelasjoner.PLASSERING);
            }
            return place;
        }
    }

    private void createCitation(Node on, RelationshipType r, CitationWithSource c) {
        Node kilde = fetchOrCreateSource(c.source);
        Relationship relasjon = on.createRelationshipTo(kilde, r);

        if (c.whereInSource != null) {
            relasjon.setProperty("sitat", c.whereInSource.value);
        }
        if (c.certainty != null) {
            relasjon.setProperty("kvalitet", c.certainty.value);
        }
    }

    private Node fetchOrCreateSource(Source source) {
        LOG.info("fetchOrCreateSource('{}')", makeId(source.xref));
        return fetchOrCreateAndPopulate(LBL_KILDE, "id", makeId(source.xref), source, (node, from) -> {
            node.setProperty("tittel", mapToStringArray(from.title));
            if (!isEmpty(from.publicationFacts)) {
                node.setProperty("publisering", mapToStringArray(from.publicationFacts));
            }
            if (!isEmpty(from.originatorsAuthors)) {
                node.setProperty("forfatter", mapToStringArray(from.originatorsAuthors));
            }
            addNotes(node, from.notes);
        });
    }

    private <T> Node fetchOrCreateAndPopulate(Label label, String keyName, String keyValue, T from, Populator<T> populator) {
        LOG.info("fetchOrCreateAndPopulate('{}', '{}', '{}')", label.name(), keyName, keyValue);
        ResourceIterable<Node> nodes = graphDb.findNodesByLabelAndProperty(label, keyName, keyValue);
        Node node;
        if (!Iterables.isEmpty(nodes)) {
            LOG.debug("Found existing node '{}'", keyValue);
            node = nodes.iterator().next();
        } else {
            LOG.debug("Creating new node '{}'", keyValue);
            node = graphDb.createNode(label);
            node.setProperty(keyName, keyValue);
            populator.populate(node, from);
        }
        return node;
    }

    private void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    private static String[] mapToStringArray(List<String> list) {
        return mapToStringArray(list, str -> str);
    }

    private static String makeId(String xref) {
        return xref.replaceAll("@", "");
    }

    private static <T> String[] mapToStringArray(List<T> list, Function<T, String> mappingFunction) {
        String[] result = new String[list.size()];

        List<String> mappedList = list.stream().map(mappingFunction).collect(toList());

        for (int index = 0; index < list.size(); index++) {
            result[index] = mappedList.get(index);
        }

        return result;
    }

    private static String mapHendelseType (String type){
        String mappedType = HENDELSE_TYPE_MAPPING.get(type);
        return mappedType != null ? mappedType : type;
    }

    private static enum FamilieRelasjoner implements RelationshipType {
        HUSTRU, EKTEMANN, BARN, HENDELSE
    }

    private static enum PersonRelasjoner implements RelationshipType {
        HENDELSE, SITAT, NAVNESITAT, MOR, FAR
    }

    private static enum HendelseRelasjoner implements RelationshipType {
        STED, SITAT
    }

    private static enum StedRelasjoner implements RelationshipType {
        PLASSERING
    }

    @FunctionalInterface
    private static interface Populator<S> {
        void populate(Node to, S from);
    }
}
