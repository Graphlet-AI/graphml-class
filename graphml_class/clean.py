from collections import defaultdict
from typing import Dict, Union
from networkx import Graph, DiGraph

#
# Graphistry has trouble with null values - this can break a visualization when coloring by value. This utility imputes nulls.
#

# Function to check if a value should be replaced
def needs_replacement(value):
    """Is it None, 'null' or null string?"""
    return value is None or value == "null" or value == ""

def clean_graph(G: Union[Graph, DiGraph]) -> Union[Graph, DiGraph]:
    """Impute empty networkx graph properties with 0s"""
    G_copy = G.copy()

    # Profile the types in fields
    field_type_count = defaultdict(defaultdict(int))
    for node, attrs in G_copy.nodes(data=True):
        for key, value in attrs.items():
            field_type_count[key][type(value)] += 1

    # Take the most common type for each
    prop_types = {}
    for property, type_count in field_type_count:
        top_type_pair = max(type_count.items(), key=lambda x: x[1])
        top_type = top_type_pair[0]
        prop_types[property] = top_type
    
    for node, attrs in G_copy.nodes(data=True):
        for key, value in attrs.items():
            if (prop_types[key] == str) and needs_replacement(value):
                G_copy.nodes[node][key] = 0
    return G_copy
