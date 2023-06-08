import gzip
import io
import tarfile
from datetime import date

import networkx as nx
import requests
from utils import extract_paper_info

# Initialize a directed graph
G = nx.DiGraph()

# Download and load edges (citations) from `cit-HepTh.txt.gz`
response = requests.get("https://snap.stanford.edu/data/cit-HepTh.txt.gz")
gzip_content = io.BytesIO(response.content)

# Decompress the gzip content and build the edge list for our network
with gzip.GzipFile(fileobj=gzip_content) as f:
    for line in f:
        line = line.decode("utf-8")
        # Ignore lines that start with '#'
        if not line.startswith("#"):
            cited, citing = line.strip().split("\t")
            G.add_edge(citing, cited)


# Download the abstracts from `cit-HepTh-abstracts.tar.gz`
abstract_response = requests.get("https://snap.stanford.edu/data/cit-HepTh-abstracts.tar.gz")

# Convert the response content to an in-memory binary stream
abstract_gzip_content = io.BytesIO(abstract_response.content)

# Decompress the gzip content
with gzip.GzipFile(fileobj=abstract_gzip_content) as f:
    with tarfile.open(fileobj=f, mode="r|") as tar:
        for member in tar:
            abstract_file = tar.extractfile(member)
            if abstract_file is not None:
                content = abstract_file.read().decode("utf-8")
                paper_info = extract_paper_info(content)
                if paper_info:
                    paper_id = paper_info.get("Paper", "").split("/")[
                        -1
                    ]  # Get the paper ID part of the "Paper" field
                    if paper_id in G:
                        for field, value in paper_info.items():
                            if paper_id in G:
                                G.nodes[paper_id][field] = value
                    else:
                        # Add isolated nodes if paper_id isn't in G
                        G.add_node(paper_id, **paper_info)


# Download and load edges (citations) from `cit-HepTh.txt.gz`
date_response = requests.get("https://snap.stanford.edu/data/cit-HepTh-dates.txt.gz")
gzip_content = io.BytesIO(date_response.content)

# Decompress the gzip content and add a "published" date property to our nodes
with gzip.GzipFile(fileobj=gzip_content) as f:
    for line in f:
        line = line.decode("utf-8")
        # Ignore lines that start with '#'
        if not line.startswith("#"):
            paper_id, iso_date = line.strip().split("\t")
            if paper_id in G:
                G.nodes[paper_id]["published"] = date.fromisoformat(iso_date)
