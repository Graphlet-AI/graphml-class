// index sanctioning bodies to search for OFAC
CREATE FULLTEXT INDEX SanctionIndex IF NOT EXISTS FOR (n:Sanction) ON EACH [n.authority];

CALL apoc.cypher.run("
    // 1st get the OFAC sanctioned organization nodes
    CALL db.index.fulltext.queryNodes('SanctionIndex', 'OFAC') YIELD node
                    WITH collect(node) AS sanctions
                    UNWIND sanctions AS sanction
                    // From sanctions to sanctioned entities
                    MATCH (sanction)-[e1:ENTITY]-(sanctioned:Organization)
                    WITH collect(DISTINCT sanctioned) AS sanctioned_entities
                    UNWIND sanctioned_entities AS sanctioned
                    RETURN DISTINCT(sanctioned) as node

    // Get the OFAC sanctioned organization nodes and their significant connections AT ONCE
    // by unioning the query below.
    UNION

    // Now get the nodes of their significant connections by ownership or directorship
    CALL db.index.fulltext.queryNodes('SanctionIndex', 'OFAC') YIELD node
                    WITH collect(node) AS sanctions
                    UNWIND sanctions AS sanction
                    // From sanctions to sanctioned entities
                    MATCH (sanction)-[e1:ENTITY]-(sanctioned:Organization)
                    WITH collect(DISTINCT sanctioned) AS sanctioned_entities
                    UNWIND sanctioned_entities AS sanctioned
                    MATCH (sanctioned)-[e2:OWNERSHIP|OWNER|DIRECTORSHIP|PARENT]-(other_entity)
                    RETURN DISTINCT(other_entity) as node;
", {}) YIELD value WITH COLLECT(value.node) AS all_nodes

// Use the collected nodes in another part of the query
UNWIND all_nodes AS node1
UNWIND all_nodes AS node2
MATCH (node1)-[relationship:OWNERSHIP|OWNER|DIRECTORSHIP|PARENT]-(node2)
RETURN DISTINCT node1, relationship, node2;