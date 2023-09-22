import calendar
import gzip
import io
import re
import tarfile
import typing
from datetime import date
from typing import Dict

import networkx as nx
import numpy as np
import requests
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from torch import Tensor

model = SentenceTransformer("sentence-transformers/paraphrase-MiniLM-L6-v2")


def extract_paper_info(record):
    """Extract structured information from the text of academic paper text records using regular expressions.

    Note: I was written wholly or in part by ChatGPT4 on May 23, 2023.
    """

    # Initialize an empty dictionary to hold the information
    info = {}

    # Match "Paper" field
    paper_match = re.search(r"Paper:\s*(.*)", record)
    if paper_match:
        info["Paper"] = paper_match.group(1)

    # # Match "From" field
    # from_match = re.search(r"From:\s*(.*)", record)
    # if from_match:
    #     info['From'] = from_match.group(1)

    # Match "From" field
    from_match = re.search(r"From:\s*([^<]*)<", record)
    if from_match:
        info["From"] = from_match.group(1).strip()

    # Match "Date" field
    date_match = re.search(r"Date:\s*(.*)", record)
    if date_match:
        info["Date"] = date_match.group(1)

    # Match "Title" field
    title_match = re.search(r"Title:\s*(.*)", record)
    if title_match:
        info["Title"] = title_match.group(1)

    # Match "Authors" field
    authors_match = re.search(r"Authors:\s*(.*)", record)
    if authors_match:
        info["Authors"] = authors_match.group(1)

    # Match "Comments" field
    comments_match = re.search(r"Comments:\s*(.*)", record)
    if comments_match:
        info["Comments"] = comments_match.group(1)

    # Match "Report-no" field
    report_no_match = re.search(r"Report-no:\s*(.*)", record)
    if report_no_match:
        info["Report-no"] = report_no_match.group(1)

    # Match "Journal-ref" field
    journal_ref_match = re.search(r"Journal-ref:\s*(.*)", record)
    if journal_ref_match:
        info["Journal-ref"] = journal_ref_match.group(1)

    # Extract "Abstract" field
    abstract_pattern = r"Journal-ref:[^\\\\]*\\\\[\n\s]*(.*?)(?=\\\\)"
    abstract_match = re.search(abstract_pattern, record, re.DOTALL)
    if abstract_match:
        abstract = abstract_match.group(1)
        abstract = abstract.replace("\n", " ").replace("  ", " ")
        info["Abstract"] = abstract.strip()

    return info


def embed_paper_info(
    records: typing.Union[str, typing.List[str]], convert_to_tensor=True
) -> typing.Union[np.ndarray, Tensor]:
    if records and isinstance(records, str):
        records = [records]
    return model.encode(records, convert_to_tensor=convert_to_tensor)


def compare_paper_embeddings(X, Y):
    return cosine_similarity(X, Y)


def main():
    """main Build a network in NetworkX and DGL for the "High-energy physics theory citation network" dataset."""
    # Initialize a directed graph
    G = nx.DiGraph()

    # Download and load edges (citations) from `cit-HepTh.txt.gz`
    print("Fetching citation graph ...")
    response = requests.get("https://snap.stanford.edu/data/cit-HepTh.txt.gz")
    gzip_content = io.BytesIO(response.content)

    print("Writing edge list to file ...")
    with open("data/cit-HepTh.txt.gz", "wb") as f:
        f.write(response.content)
        print("Wrote downloaded edge file to data/cit-HepTh.txt.gz")

    # We need to create sequential IDs starting from 0 for littleballoffur and DGL
    file_to_net: Dict[str, int] = {}
    net_to_file: Dict[int, str] = {}
    current_idx = 0

    # Decompress the gzip content and build the edge list for our network
    print("Building network structure ...")
    with gzip.GzipFile(fileobj=gzip_content) as f:
        for line_number, line in enumerate(f):
            line = line.decode("utf-8")

            # Ignore comment lines that start with '#'
            if not line.startswith("#"):
                # Source (citing), desstination (cited) papers
                citing_key, cited_key = line.strip().split("\t")

                # The edge list makes the paper ID an int, stripping 0001001 to 1001, for example
                citing_key, cited_key = int(citing_key), int(cited_key)

                # If the either of the paper IDs don't exist, make one
                for key in [citing_key, cited_key]:
                    if key not in file_to_net:
                        # Build up an index that maps back and forth
                        file_to_net[key] = current_idx
                        net_to_file[current_idx] = key

                        # Bump the current ID
                        current_idx += 1

                # print(f"Citing key: {citing_key}, Cited key: {cited_key}")
                # print(f"Mapped key: {file_to_net[citing_key]}, Mapped key: {file_to_net[cited_key]}")

                G.add_edge(file_to_net[citing_key], file_to_net[cited_key], edge_id=line_number)

                # Conditionally set the keys on the nodes
                G.nodes[file_to_net[citing_key]]["file_id"] = citing_key
                G.nodes[file_to_net[citing_key]]["sequential_id"] = file_to_net[citing_key]

                G.nodes[file_to_net[cited_key]]["file_id"] = cited_key
                G.nodes[file_to_net[cited_key]]["sequential_id"] = file_to_net[cited_key]

    # Download the abstracts from `cit-HepTh-abstracts.tar.gz`
    print("Fetching paper abstracts ...")
    abstract_response = requests.get("https://snap.stanford.edu/data/cit-HepTh-abstracts.tar.gz")
    abstract_gzip_content = io.BytesIO(abstract_response.content)

    with open("data/cit-HepTh-abstracts.tar.gz", "wb") as f:
        f.write(abstract_response.content)
        print("Wrote downloaded abstract file to data/cit-HepTh-abstracts.tar.gz")

    hit_count, miss_count = 0, 0
    # Decompress the gzip content, then work through the abstract files in the tarball
    with gzip.GzipFile(fileobj=abstract_gzip_content) as f:
        with tarfile.open(fileobj=f, mode="r|") as tar:
            for file_number, member in enumerate(tar):
                abstract_file = tar.extractfile(member)
                if abstract_file is not None:
                    content = abstract_file.read().decode("utf-8")

                    # We can also parse and use those values directly or embed field-wise
                    paper_info = extract_paper_info(content)
                    if paper_info:
                        paper_id = paper_info.get("Paper", "").split("/")[-1]

                        # The edge list makes the paper ID an int, stripping 0001001 to 1001, for example
                        paper_id = int(paper_id)

                        # Get the paper ID part of the "Paper" field
                        if paper_id in file_to_net and file_to_net[paper_id] in G:
                            for field, value in paper_info.items():
                                G.nodes[file_to_net[paper_id]][field] = value
                            hit_count += 1

                        else:
                            # Add isolated nodes if paper_id isn't in G
                            miss_count += 1
                            # G.add_node(file_to_net[paper_id], **paper_info)

    # Now `G` is a property graph representing the "High-energy physics theory citation network" dataset
    print(f"Added metadata to {hit_count:,} nodes, {miss_count:,} were unknown.")

    # Download and load edges (citations) from `cit-HepTh.txt.gz`
    date_response = requests.get("https://snap.stanford.edu/data/cit-HepTh-dates.txt.gz")
    date_gzip_content = io.BytesIO(date_response.content)

    with open("data/cit-HepTh-dates.txt.gz", "wb") as f:
        f.write(date_response.content)
        print("Wrote downloaded publishing dates file to data/cit-HepTh-dates.txt.gz")

    # Decompress the gzip content and add a "published" date property to our nodes
    print("Adding publising dates ...")
    with gzip.GzipFile(fileobj=date_gzip_content) as f:
        for line in f:
            line = line.decode("utf-8")
            # Ignore lines that start with '#'
            if not line.startswith("#"):
                paper_id, iso_date = line.strip().split("\t")

                # The edge list makes the paper ID an int, stripping 0001001 to 1001, for example
                paper_id = int(paper_id)

                if paper_id in file_to_net and file_to_net[paper_id] in G:
                    # Add a UTC timestamp for the data
                    G.nodes[file_to_net[paper_id]]["Published"] = calendar.timegm(
                        date.fromisoformat(iso_date).timetuple()
                    )

    # What is the first node?
    test_g = G.nodes[0]
    # print([G.nodes[i] for i in range(0, 10)])
    print(test_g)
    assert test_g["sequential_id"] == 0
    assert test_g["file_id"] == 1001

    # # Now embed some of the node content
    # abstracts = [node["Abstract"] for node in G.nodes()]
    # for node in G.nodes():

    #     # Embedding is a generic approach with or without parsing.
    #     content_embedding = embed_paper_info(content, convert_to_tensor=False)
    #     G.nodes[file_to_net[paper_id]]["embedding"] = content_embedding[0]

    #     paper_title = G.nodes[file_to_net[paper_id]]["Title"]
    #     title_embedding = embed_paper_info(paper_title, convert_to_tensor=False)
    #     G.nodes[file_to_net[paper_id]]["title_embedding"] = title_embedding[0]


if __name__ == "__main__":
    main()
