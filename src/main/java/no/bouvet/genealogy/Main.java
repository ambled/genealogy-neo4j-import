package no.bouvet.genealogy;

public class Main {

    public static void main(String... args) throws Exception {
        new GedcomToNeo4J().load("src/main/resources/min-slekt.ged", "neo4j-test");
    }
}
