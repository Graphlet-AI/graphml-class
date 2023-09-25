import typing

from dgl import DGLGraph
from dgl.dataloading import GraphDataLoader
from graphml_class.citation import (
    CitationGraphDataset,
    embed_paper_info,
    extract_paper_info,
)
from pytest import fixture
from sklearn.metrics.pairwise import cosine_similarity
from torch import Tensor

print("I take a moment to download the model for encoding sentences the first time I run...")


@fixture
def get_docs():
    docs = [
        """------------------------------------------------------------------------------
\\
Paper: hep-th/9612115
From: Asato Tsuchiya <tsuchiya@theory.kek.jp>
Date: Wed, 11 Dec 1996 17:38:56 +0900   (20kb)
Date (revised): Tue, 31 Dec 1996 01:06:34 +0900

Title: A Large-N Reduced Model as Superstring
Authors: N. Ishibashi, H. Kawai, Y. Kitazawa and A. Tsuchiya
Comments: 29 pages, Latex, a footnote and references added, eq.(3.52)
  corrected, minor corrections
Report-no: KEK-TH-503, TIT/HEP-357
Journal-ref: Nucl.Phys. B498 (1997) 467-491
\\
  A matrix model which has the manifest ten-dimensional N=2 super Poincare
invariance is proposed. Interactions between BPS-saturated states are analyzed
to show that massless spectrum is the same as that of type IIB string theory.
It is conjectured that the large-N reduced model of ten-dimensional super
Yang-Mills theory can be regarded as a constructive definition of this model
and therefore is equivalent to superstring theory.
\\
""",
        """------------------------------------------------------------------------------
\\
Paper: hep-th/9711029
From: John Schwarz <jhs@theory.caltech.edu>
Date: Wed, 5 Nov 1997 17:30:55 GMT   (20kb)
Date (revised v2): Thu, 6 Nov 1997 23:52:45 GMT   (21kb)

Title: The Status of String Theory
Author: John H. Schwarz
Comments: 16 pages, latex, two figures; minor corrections, references added
Report-no: CALT-68-2140
\\
  There have been many remarkable developments in our understanding of
superstring theory in the past few years, a period that has been described as
``the second superstring revolution.'' Several of them are discussed here. The
presentation is intended primarily for the benefit of nonexperts.
\\
""",
    ]
    return docs


def test_extract_paper_info(get_docs: typing.List[str]) -> None:
    """test_extract_paper_info Test extracting fields from paper info

    Parameters
    ----------
    get_docs : typing.List[str]
        fixture containing two paper documents
    """
    (doc1, doc2) = get_docs

    paper_info = extract_paper_info(doc1)
    # Get the paper ID part of the "Paper" field
    paper_id = int(paper_info.get("Paper", "").split("/")[-1])
    assert paper_info["Paper"] == "hep-th/9612115"
    assert paper_id == 9612115

    paper_info = extract_paper_info(doc2)
    # Get the paper ID part of the "Paper" field
    paper_id = int(paper_info.get("Paper", "").split("/")[-1])
    assert paper_info["Paper"] == "hep-th/9711029"
    assert paper_id == 9711029


def test_embed_paper_info(get_docs: typing.List[str]) -> None:
    """test_embed_paper_info Test embedding our paper documents into vector embeddings.

    Parameters
    ----------
    get_docs : typing.List[str]
        fixture containing two paper documents
    """
    emb_docs = embed_paper_info(get_docs, convert_to_tensor=False)
    assert emb_docs.shape == (2, 384)
    assert (
        cosine_similarity(emb_docs[0].reshape(1, -1), emb_docs[1].reshape(1, -1))[0, 0] - 0.7496432
    ) < 0.0001
    emb_docs = embed_paper_info(get_docs, convert_to_tensor=True)
    assert isinstance(emb_docs, Tensor)


def test_citation_dgl_graph(get_docs: typing.List[str]) -> None:
    """test_citation_dgl_graph Test instantiating our CitationDGLGraph class.

    Parameters
    ----------
    get_docs : typing.List[str]
        _description_
    """
    dataset = CitationGraphDataset()
    dataloader = GraphDataLoader(dataset, batch_size=1, shuffle=True)
    g, labels = next(dataloader)
    assert isinstance(g, DGLGraph)
    assert isinstance(labels, Tensor)
