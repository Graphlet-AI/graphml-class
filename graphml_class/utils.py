import gzip
import io
import re
import tarfile
import typing
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
    """main _summary_"""
    # Initialize a directed graph
    G = nx.DiGraph()

    # Download and load edges (citations) from `cit-HepTh.txt.gz`
    response = requests.get("https://snap.stanford.edu/data/cit-HepTh.txt.gz")
    gzip_content = io.BytesIO(response.content)

    with open("data/cit-HepTh.txt.gz", "wb") as f:
        f.write(response.content)
        print("Wrote downloaded edge file to data/cit-HepTh.txt.gz")

    # We need to create sequential IDs starting from 0 for littleballoffur
    file_to_fur: Dict[int, int] = {}
    fur_to_file: Dict[int, int] = {}
    current_idx = 0

    # Decompress the gzip content and build the edge list for our network
    with gzip.GzipFile(fileobj=gzip_content) as f:
        for line_number, line in enumerate(f):
            line = line.decode("utf-8")

            # Ignore comment lines that start with '#'
            if not line.startswith("#"):
                # Source (citing), desstination (cited) papers
                citing_key, cited_key = line.strip().split("\t")
                citing_key, cited_key = int(citing_key), int(cited_key)

                # If the either of the paper IDs don't exist, make one
                for key in [citing_key, cited_key]:
                    if key not in file_to_fur:
                        # Build up an index that maps back and forth
                        file_to_fur[key] = current_idx
                        fur_to_file[current_idx] = key

                        # Bump the current ID
                        current_idx += 1

                # print(f"Citing key: {citing_key}, Cited key: {cited_key}")
                # print(f"Mapped key: {file_to_fur[citing_key]}, Mapped key: {file_to_fur[cited_key]}")

                G.add_edge(file_to_fur[citing_key], file_to_fur[cited_key])

                # Conditionally set the keys on the nodes
                G.nodes[file_to_fur[citing_key]]["file_id"] = citing_key
                G.nodes[file_to_fur[citing_key]]["sequential_id"] = file_to_fur[citing_key]

                G.nodes[file_to_fur[cited_key]]["file_id"] = cited_key
                G.nodes[file_to_fur[cited_key]]["sequential_id"] = file_to_fur[cited_key]
