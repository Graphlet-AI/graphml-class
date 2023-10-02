import gzip
import os
import tarfile
from typing import Dict, List

import dgl
import numpy as np
import torch
from dgl.data import DGLDataset
from dgl.data.utils import download as dgl_download
from dgl.data.utils import load_graphs, save_graphs
from dgl.sampling import global_uniform_negative_sampling
from sentence_transformers import SentenceTransformer


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
        self._paraphrase_model = SentenceTransformer(
            "sentence-transformers/paraphrase-MiniLM-L6-v2"
        )
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

        # Embed all the abstracts at once with a paraphrase model
        node_ids = list(abstracts.keys())
        contents = list(abstracts.values())

        if contents and isinstance(contents, str):
            contents = [contents]
        all_embeddings = self._paraphrase_model.encode(contents, convert_to_tensor=False)

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
        self.graph = dgl.graph((torch.tensor(u), torch.tensor(v)))
        self.graph.ndata["feat"] = self.featurize(file_to_net)

        #
        # Build label set for link prediction: balanced positive / negative, count = number of edges
        #

        # The real edges are the positive labels
        pos_src, pos_dst = self.graph.edges()

        # Negative sample labels sized by the number of actual, positive edges
        neg_src, neg_dst = global_uniform_negative_sampling(
            self.graph, self.graph.number_of_edges()
        )

        # Combine positive and negative samples
        self.src_nodes = torch.cat([pos_src, neg_src])
        self.dst_nodes = torch.cat([pos_dst, neg_dst])

        # Generate labels: 1 for positive and 0 for negative samples
        self.graph.ndata["label"] = torch.cat(
            [torch.ones_like(pos_src), torch.zeros_like(neg_src)]
        ).float()

    def __getitem__(self, idx):
        assert idx == 0, "This dataset has only one graph"
        return self.graph

    def __len__(self):
        """__len__ we are one graph long"""
        return 1

    def save(self):
        """save Save our one graph to directory `self.save_path`"""
        save_graphs(self._save_path, [self.graph])

    def load(self):
        """load Load processed data from directory `self.save_path`"""
        self.graph = load_graphs(self._save_path)[0]

    def has_cache(self):
        """has_cache Check whether there are processed data in `self.save_path`"""
        return os.path.exists(self._save_path)
