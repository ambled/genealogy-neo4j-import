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
    private final Label LBL_FAMILY = DynamicLabel.label("Family");
    private final Label LBL_EVENT = DynamicLabel.label("Event");
    private final Label LBL_PLACE = DynamicLabel.label("Place");
    private final Label LBL_SOURCE = DynamicLabel.label("Source");

    private static Map<String, String> EVENT_TYPE_MAPPING = Maps.newHashMap();

    static {
        EVENT_TYPE_MAPPING.put("EVEN", "Event");
        EVENT_TYPE_MAPPING.put("BIRT", "Birth");
        EVENT_TYPE_MAPPING.put("DEAT", "Death");
        EVENT_TYPE_MAPPING.put("OCCU", "Occupation");
        EVENT_TYPE_MAPPING.put("RESI", "Residence");
        EVENT_TYPE_MAPPING.put("BAPM", "Baptism");
        EVENT_TYPE_MAPPING.put("ADOP", "Adoption");
        EVENT_TYPE_MAPPING.put("CENS", "Census");
        EVENT_TYPE_MAPPING.put("MARR", "Marriage");
        EVENT_TYPE_MAPPING.put("BURI", "Burial");
        EVENT_TYPE_MAPPING.put("PROB", "Probate");
        EVENT_TYPE_MAPPING.put("CONF", "Confirmation");
        EVENT_TYPE_MAPPING.put("ENGA", "Engagement");
        EVENT_TYPE_MAPPING.put("NATI", "Nationality");
        EVENT_TYPE_MAPPING.put("IMMI", "Immigration");
        EVENT_TYPE_MAPPING.put("NATU", "Naturalization");
        EVENT_TYPE_MAPPING.put("DIV", "Divorce");
        EVENT_TYPE_MAPPING.put("DIVF", "Divorce_Filed");
        EVENT_TYPE_MAPPING.put("RELI", "Religion");
        EVENT_TYPE_MAPPING.put("RETI", "Retirement");
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

                Node mother = createFamilyRelationship(family, f.wife, FamilyRelationship.WIFE);
                Node father = createFamilyRelationship(family, f.husband, FamilyRelationship.HUSBAND);
                f.children.forEach(c -> {
                    Node child = createFamilyRelationship(family, c, FamilyRelationship.CHILD);
                    createParentRelationship(child, mother, PersonRelationship.MOTHER, makeId(f.xref));
                    createParentRelationship(child, father, PersonRelationship.FATHER, makeId(f.xref));
                });
                f.events.forEach(e -> family.createRelationshipTo(createEvent(e, e.type.tag), FamilyRelationship.EVENT));
            });
            tx.success();
        }
    }

    private void createParentRelationship(Node child, Node parent, RelationshipType relation, String familyRef) {
        if (parent != null) {
            LOG.info("createParentRelationship(({})-[:{}]->({})): Family {}",
                    new Object[]{child.getProperty("id"), relation.name(), parent.getProperty("id"), familyRef});
            child.createRelationshipTo(parent, relation).setProperty("family", familyRef);
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
            node.setProperty("name", mapToStringArray(individual.names, n -> n.basic.trim()));

            if (individual.sex != null) {
                node.setProperty("sex", individual.sex.value);
            }
            addNotes(node, individual.notes);
            addCitations(PersonRelationship.SITAT, individual.citations, node);

            individual.attributes.forEach(a -> node.createRelationshipTo(createEvent(a, a.type.tag), PersonRelationship.EVENT));
            individual.events.forEach(e -> node.createRelationshipTo(createEvent(e, e.type.tag), PersonRelationship.EVENT));
            individual.names.forEach(n -> addCitations(PersonRelationship.NAVNESITAT, n.citations, node));
        });
    }

    private void addNotes(Node individual, List<Note> notes) {
        if (!isEmpty(notes)) {
            individual.setProperty("notater", mapToStringArray(notes, str -> Joiner.on(' ').join(str.lines)));
        }
    }

    private Node createEvent(Event e, String type) {
        Node attribute = graphDb.createNode(LBL_EVENT);
        attribute.setProperty("type", mapEventType(type));

        if (e.date != null) {
            attribute.setProperty("date", e.date.value);
        }
        if (e.place != null) {
            attribute.createRelationshipTo(fetchOrCreatePlace(e.place), EventRelationship.PLACE);
        }
        if (e.description != null && e.description.value != null) {
            attribute.setProperty("description", e.description.value);
        }
        addNotes(attribute, e.notes);
        addCitations(EventRelationship.SITAT, e.citations, attribute);

        return attribute;
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

        List<Node> candidates = Lists.newArrayList(graphDb.findNodesByLabelAndProperty(LBL_PLACE, "name", places.get(0)));
        LOG.trace("candidates: {}", candidates);
        Node node = candidates.stream().filter(candidate -> {
            List<Node> result = Lists.newArrayList(graphDb.traversalDescription()
                    .relationships(PlaceRelationship.PLASSERING, Direction.OUTGOING)
                    .traverse(candidate)
                    .nodes());

            return result.stream().map(place -> place.getProperty("name")).collect(toList()).equals(places);
        }).findAny().orElse(null);

        if (node != null) {
            LOG.debug("Found existing place '{}'", places);
            return node;
        } else {
            LOG.debug("Creating new place '{}'", places.get(0));

            Node place = graphDb.createNode(LBL_PLACE);
            place.setProperty("name", places.get(0));
            if (places.size() > 1) {
                place.createRelationshipTo(fetchOrCreatePlaceChain(places.subList(1, places.size())), PlaceRelationship.PLASSERING);
            }
            return place;
        }
    }

    private void createCitation(Node on, RelationshipType r, CitationWithSource c) {
        Node Source = fetchOrCreateSource(c.source);
        Relationship relation = on.createRelationshipTo(Source, r);

        if (c.whereInSource != null) {
            relation.setProperty("sitat", c.whereInSource.value);
        }
        if (c.certainty != null) {
            relation.setProperty("certainty", c.certainty.value);
        }
    }

    private Node fetchOrCreateSource(Source source) {
        LOG.info("fetchOrCreateSource('{}')", makeId(source.xref));
        return fetchOrCreateAndPopulate(LBL_SOURCE, "id", makeId(source.xref), source, (node, from) -> {
            node.setProperty("title", mapToStringArray(from.title));
            if (!isEmpty(from.publicationFacts)) {
                node.setProperty("publicationFacts", mapToStringArray(from.publicationFacts));
            }
            if (!isEmpty(from.originatorsAuthors)) {
                node.setProperty("author", mapToStringArray(from.originatorsAuthors));
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

    private static String mapEventType (String type){
        String mappedType = EVENT_TYPE_MAPPING.get(type);
        return mappedType != null ? mappedType : type;
    }

    private static enum FamilyRelationship implements RelationshipType {
        WIFE, HUSBAND, CHILD, EVENT
    }

    private static enum PersonRelationship implements RelationshipType {
        EVENT, SITAT, NAVNESITAT, MOTHER, FATHER
    }

    private static enum EventRelationship implements RelationshipType {
        PLACE, SITAT
    }

    private static enum PlaceRelationship implements RelationshipType {
        PLASSERING
    }

    @FunctionalInterface
    private static interface Populator<S> {
        void populate(Node to, S from);
    }
}
