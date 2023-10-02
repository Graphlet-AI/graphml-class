import calendar
import gzip
import io
import json
import os
import re
import tarfile
from datetime import date
from typing import Dict, List, Union

import dgl
import networkx as nx
import numpy as np
import pandas as pd
import requests
import torch
import umap
from dgl.data import DGLDataset
from dgl.data.utils import download as dgl_download
from dgl.data.utils import load_graphs, save_graphs
from dgl.sampling import global_uniform_negative_sampling
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
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
    date_match = re.search(r"Date:\s*(.*)(\s*)(\(\d+kb\))", record)
    if date_match:
        info["Date"] = date_match.group(1).strip()

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
    records: Union[str, List[str]], convert_to_tensor=True
) -> Union[np.ndarray, Tensor]:
    if records and isinstance(records, str):
        records = [records]
    return model.encode(records, convert_to_tensor=convert_to_tensor)


def main():
    """main Build a network in NetworkX and DGL for the "High-energy physics theory citation network" dataset."""
    # Initialize a directed graph
    G = nx.DiGraph()

    # Download and load edges (citations) from `cit-HepTh.txt.gz`
    edge_path = "data/cit-HepTh.txt.gz"
    gzip_content = None

    if os.path.exists(edge_path):
        print(f"Using existing citation graph edge file {edge_path}")
        gzip_content = open(edge_path, "rb")
    else:
        print("Fetching citation graph edge file ...")
        response = requests.get(f"https://snap.stanford.edu/{edge_path}")
        gzip_content = io.BytesIO(response.content)

        print("Writing edge list to file {edge_path}")
        with open(edge_path, "wb") as f:
            f.write(response.content)
            print(f"Wrote downloaded edge file to {edge_path}")

    # We need to create sequential IDs starting from 0 for littleballoffur and DGL
    file_to_net: Dict[int, int] = {}
    net_to_file: Dict[int, int] = {}
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
    abstract_path = "data/cit-HepTh-abstracts.tar.gz"
    abstract_gzip_content = None

    if os.path.exists(abstract_path):
        print(f"Using existing paper abstracts file {abstract_path}")
        with open(abstract_path, "rb") as f:
            abstract_gzip_content = io.BytesIO(f.read())
    else:
        print("Downloading paper abbstracts ...")
        abstract_response = requests.get(f"https://snap.stanford.edu/{abstract_path}")
        abstract_gzip_content = io.BytesIO(abstract_response.content)

        print(f"Downloading abstract file to {abstract_path}")
        with open(abstract_path, "wb") as f:
            f.write(abstract_response.content)
            print(f"Wrote downloaded abstract file to {abstract_path}")

    hit_count, miss_count, matches = 0, 0, 0
    all_abstracts: List[str] = []
    abstracts: Dict[int, str] = {}
    paper_ids: List[int] = []
    # Decompress the gzip content, then work through the abstract files in the tarball
    with gzip.GzipFile(fileobj=abstract_gzip_content) as f:
        with tarfile.open(fileobj=f, mode="r|") as tar:
            for member in tar:
                abstract_file = tar.extractfile(member)
                if abstract_file:
                    content = abstract_file.read().decode("utf-8")

                    paper_id = int(os.path.basename(member.name).split(".")[0])

                    # We can also parse and use those values directly or embed field-wise
                    paper_info = extract_paper_info(content)
                    if paper_info:
                        abstract_paper_id = paper_info.get("Paper", "").split("/")[-1]
                        if paper_id != int(abstract_paper_id):
                            matches += 1
                            print(f"Paper ID {paper_id} != {abstract_paper_id}")

                        # Get the paper ID part of the "Paper" field
                        if paper_id in file_to_net and file_to_net[paper_id] in G:
                            for field, value in paper_info.items():
                                G.nodes[file_to_net[paper_id]][field] = value

                            abstracts[paper_id] = content
                            all_abstracts.append(content)
                            paper_ids.append(paper_id)

                            hit_count += 1

                        else:
                            # Add isolated nodes if paper_id isn't in G
                            miss_count += 1
                            # G.add_node(file_to_net[paper_id], **paper_info)

    # Now `G` is a property graph representing the "High-energy physics theory citation network" dataset
    print(f"Added metadata to {hit_count:,} nodes, {miss_count:,} were unknown.")

    # Download and load edges (citations) from `cit-HepTh.txt.gz`
    dates_path = "data/cit-HepTh-dates.txt.gz"
    date_gzip_content = None

    if os.path.exists(dates_path):
        print(f"Using existing paper dates file {dates_path}")
        date_gzip_content = open(dates_path, "rb")
    else:
        print("Downloading paper publishing dates ...")
        date_response = requests.get(f"https://snap.stanford.edu/{dates_path}")
        date_gzip_content = io.BytesIO(date_response.content)

        with open(dates_path, "wb") as f:
            f.write(date_response.content)
            print("Wrote downloaded publishing dates file to {dates_path}")

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
    test_node = G.nodes[0]
    print(test_node)
    assert test_node["sequential_id"] == 0
    assert test_node["file_id"] == 1001

    # Embed the abstracts for GNN features. Embedding is a generic approach for retrieval as well.
    # Note: NetworkX can't save lists in GEXF format, so we'll JSONize the list & save the embeddings separately.
    embedded_abstracts: np.ndarray = None
    if os.path.exists("data/embedded_abstracts.npy"):
        embedded_abstracts = np.load("data/embedded_abstracts.npy")
    else:
        embedded_abstracts = embed_paper_info(all_abstracts, convert_to_tensor=False)
        np.save("data/embedded_abstracts.npy", embedded_abstracts)

    node_embedding_dict: Dict[int, List[float]] = {}
    if os.path.exists("data/node_embedding_dict.json.gz"):
        node_embedding_dict = json.load(
            gzip.GzipFile("data/node_embedding_dict.json.gz", "r"),
            # encoding="utf-8",
        )
    else:
        for paper_id, emb in zip(paper_ids, embedded_abstracts):
            assert emb.shape == (384,)

            # Gephi assumes a list of floats is a time series, so we need to convert to a string
            emb_list = emb.tolist()
            G.nodes[file_to_net[paper_id]]["Embedding-JSON"] = json.dumps(emb_list)

            node_embedding_dict[paper_id] = emb_list

    # Write the mapping from paper ID to embedding to JSON.
    # Note: All JSON keys are strings. We will have to int(key) to read the data back.
    json.dump(
        node_embedding_dict,
        io.TextIOWrapper(
            gzip.GzipFile("data/node_embedding_dict.json.gz", "w"),
            encoding="utf-8",
        ),
        indent=4,
        sort_keys=True,
    )

    # Write the entire network using GEXF format - the date has to be in UTC format for this to work.
    nx.write_gexf(G, path="data/physics_embeddings.gexf", prettyprint=True)

    #
    # Create a pd.DataFrame of the nodes for analysis in a notebook
    #

    # Extract nodes and their attributes into a list of dictionaries
    node_data = [{**{"node": node}, **attr} for node, attr in G.nodes(data=True)]

    # Convert the list of dictionaries into a DataFrame
    node_df = pd.DataFrame(node_data)

    # Cleanup
    node_df.fillna("", inplace=True)

    # Embed the dirty Journal-ref and cluster it to produce labels.
    model = SentenceTransformer("sentence-transformers/paraphrase-MiniLM-L6-v2")

    # Embed these columns
    for column in ["Journal-ref", "Title", "Abstract"]:
        embeddings = model.encode(node_df[column].tolist())
        node_df[f"{column}Embedding"] = embeddings.tolist()

    reducer = umap.UMAP()


class CitationGraphDataset(DGLDataset):
    """CitationGraphDataset DGLDataset sub-class for loading our citation network dataset.

    Parameters
    ----------
    url : str
        URL of the base path at SNAP from which to download the raw dataset(s)
    raw_dir : str
        Specifying the directory that will store the
        downloaded data or the directory that
        already stores the input data.
        Default: ~/.dgl/
    save_dir : str
        Directory to save the processed dataset.
        Default: the value of `raw_dir`
    force_reload : bool
        Whether to reload the dataset. Default: False
    verbose : bool
        Whether to print out progress information
    """

    name = "cit-HepTh"

    def __init__(
        self,
        url="https://snap.stanford.edu/data/",
        raw_dir="data",
        save_dir="data",
        force_reload=False,
        verbose=False,
    ):
        self._save_path = os.path.join(save_dir, f"{CitationGraphDataset.name}.bin")
        super(CitationGraphDataset, self).__init__(
            name=CitationGraphDataset.name,
            url=url,
            raw_dir=raw_dir,
            save_dir=save_dir,
            force_reload=force_reload,
            verbose=verbose,
        )

    def download(self):
        """download Download all three files: edges, abstracts, and publishing dates."""
        file_names: List[str] = [
            "cit-HepTh.txt.gz",
            "cit-HepTh-abstracts.tar.gz",
            "cit-HepTh-dates.txt.gz",
        ]
        for file_name in file_names:
            file_path = os.path.join(self.raw_dir, file_name)
            dgl_download(self.url + file_name, path=file_path)

    def featurize(self, file_to_net: Dict[int, int]) -> torch.Tensor:
        """featurize Sentence encode abstracts into 384-dimension embeddings via paraphrase-MiniLM-L6-v2.

        Returns
        -------
        torch.Tensor
            (node_count,384) tensor of abstract embeddings
        """
        self.model = SentenceTransformer("sentence-transformers/paraphrase-MiniLM-L6-v2")

        abstracts: Dict[int, str] = {}

        # Decompress the gzip content, then work through the abstract files in the tarball
        abstract_path = os.path.join(self.raw_dir, "cit-HepTh-abstracts.tar.gz")
        with gzip.GzipFile(filename=abstract_path) as f:
            with tarfile.open(fileobj=f, mode="r|") as tar:
                for member in tar:
                    paper_id = int(os.path.basename(member.name).split(".")[0])

                    abstract_file = tar.extractfile(member)
                    if abstract_file and paper_id in file_to_net:
                        content = abstract_file.read().decode("utf-8")
                        abstracts[file_to_net[paper_id]] = content

        # Embed all the abstracts at once
        node_ids = list(abstracts.keys())
        contents = list(abstracts.values())
        all_embeddings = embed_paper_info(contents, convert_to_tensor=False)

        # Determine the max node ID to ensure the embeddings tensor covers all nodes
        max_node_id = max(abstracts.keys())

        # Pre-allocate a tensor filled with zeros for all node embeddings
        embeddings = np.zeros((max_node_id + 1, 384))

        # Fill in the tensor with the embeddings at the corresponding node IDs
        for idx, node_id in enumerate(node_ids):
            embeddings[node_id] = all_embeddings[idx]

        return torch.tensor(embeddings)

    def process(self):
        """process Build graph and node features from sbert on raw data."""

        #
        # Build the graph U/V edge Tensors
        #
        u, v = [], []
        current_idx = 0
        edge_path = os.path.join(self.raw_dir, "cit-HepTh.txt.gz")
        file_to_net: Dict[int, int] = {}
        with gzip.GzipFile(filename=edge_path) as f:
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

                            # Bump the current ID
                            current_idx += 1

                    u.append(file_to_net[citing_key])
                    v.append(file_to_net[cited_key])

        # Build our DGLGraph from the edge list :)
        self._g = dgl.graph((torch.tensor(u), torch.tensor(v)))
        self._g.ndata["x"] = self.featurize(file_to_net)

        #
        # Build label set for link prediction: balanced positive / negative, count = number of edges
        #

        # The real edges are the positive labels
        pos_src, pos_dst = self._g.edges()

        # Negative sample labels sized by the number of actual, positive edges
        neg_src, neg_dst = global_uniform_negative_sampling(self._g, self._g.number_of_edges())

        # Combine positive and negative samples
        self.src_nodes = torch.cat([pos_src, neg_src])
        self.dst_nodes = torch.cat([pos_dst, neg_dst])

        # Generate labels: 1 for positive and 0 for negative samples
        self.labels = torch.cat([torch.ones_like(pos_src), torch.zeros_like(neg_src)]).float()

    def __getitem__(self, idx):
        assert idx == 0, "This dataset has only one graph"
        return self._g

    def __len__(self):
        """__len__ we are one graph long"""
        return 1

    def save(self):
        """save Save our one graph to directory `self.save_path`"""
        save_graphs(self._save_path, [self._g])

    def load(self):
        """load Load processed data from directory `self.save_path`"""
        self._g = load_graphs(self._save_path)[0]

    def has_cache(self):
        """has_cache Check whether there are processed data in `self.save_path`"""
        return os.path.exists(self._save_path)


if __name__ == "__main__":
    main()
