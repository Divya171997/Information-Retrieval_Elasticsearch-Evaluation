"""Interactive Elasticsearch search engine with a simple precision/recall report.

Updated copy of ``searchengine.py`` (the original file is kept unchanged) for the
Elasticsearch 8 Python client. Behaviour is the same as the original:

* index the first 7,999 articles of the Signal Media ``sample-1M.jsonl`` news dataset,
* offer the same ten query types from a menu,
* print each hit and the same precision / recall table and averages.

What changed, and why:

* ``doc_type`` was removed from Elasticsearch in 8.x, so it is no longer passed.
* The host, index name and data file are configurable (``--host``, ``--index``, ``--data``).
* Indexing uses the bulk helper and stops reading the file once the limit is reached
  (the original kept reading the remaining ~1M lines).
* An invalid menu choice re-prompts instead of raising ``UnboundLocalError``.
* ``json_lines`` is no longer required; the file is read line by line with ``json``.
* Unused imports (``SnowballStemmer``) and the broken ``search_by_index`` helper,
  which referenced an undefined variable, were replaced by a working ``get_by_id``.

Usage::

    python searchengine_es8.py --data sample-1M.jsonl --host http://localhost:9200
"""

import argparse
import json

import numpy as np

INDEX_LIMIT = 8000          # the original indexes ids 1..7999
N_DOCS = 8000               # denominator used by the original evaluation
DEFAULT_INDEX = "news_article_dataset"


def connect(host):
    from elasticsearch import Elasticsearch
    return Elasticsearch(host)


def read_articles(path, limit=INDEX_LIMIT):
    """Yield (id, article) for ids 1..limit-1, matching the original indexing loop."""
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            if i >= limit:
                break
            line = line.strip()
            if line:
                yield i, json.loads(line)


def indexing(elastic, path, index=DEFAULT_INDEX):
    from elasticsearch import helpers
    actions = ({"_index": index, "_id": i, "_source": doc} for i, doc in read_articles(path))
    ok, _ = helpers.bulk(elastic, actions)
    elastic.indices.refresh(index=index)
    print("Indexed", ok, "documents into", index)


def summary(result):
    for data in result["hits"]["hits"]:
        print("Document Index is:", data["_id"])
        print("Document Search Score is:", data["_score"])
        print("Document Media Type:", data["_source"]["media-type"])
        print("Document Title:", data["_source"]["title"])
        print("Source:", data["_source"]["source"])
        print("Published on:", data["_source"]["published"])
        print("Document Content:", data["_source"]["content"], "\n")


def get_by_id(elastic, doc_id, index=DEFAULT_INDEX):
    return elastic.get(index=index, id=doc_id)


def _date_range(start, end):
    return {"range": {"published": {"gte": start, "lte": end, "format": "yyyy/MM/dd||yyyy"}}}


def build_query(ch, ask=input):
    """Return the Elasticsearch query for menu choice ``ch``, or None if the choice is invalid.

    The ten queries are identical to the original script.
    """
    if ch == "1":
        title = ask("Enter the Title:")
        start = ask("Enter the Published Start Date (YYYY/MM/DD):")
        end = ask("Enter the Published End Date (YYYY/MM/DD):")
        return {"bool": {"should": [{"match": {"title": title}}, _date_range(start, end)]}}
    if ch == "2":
        title = ask("Enter the Title:")
        content = ask("Enter the Content Phrase:")
        return {"bool": {"should": [{"match": {"title": title}}, {"match": {"content": content}}]}}
    if ch == "3":
        media = ask("Enter the Media-type:")
        content = ask("Enter the Content Phrase:")
        start = ask("Enter the Published Start Date (YYYY/MM/DD):")
        end = ask("Enter the Published End Date (YYYY/MM/DD):")
        return {"bool": {"should": [{"match": {"media-type": media}},
                                    {"match": {"content": content}},
                                    _date_range(start, end)]}}
    if ch == "4":
        title = ask("Enter the Title:")
        content = ask("Enter the Content Start Phrase:")
        return {"bool": {"should": [{"match": {"title": title}},
                                    {"match_phrase_prefix": {"content": content}}]}}
    if ch == "5":
        title = ask("Enter the Exact Title:")
        return {"match_phrase": {"title": title}}
    if ch == "6":
        keyword = ask("Enter Query Keyword:")
        return {"multi_match": {"query": keyword, "fields": ["content^2", "title"]}}
    if ch == "7":
        content = ask("Enter the Letter:") + "*"
        return {"wildcard": {"content": content}}
    if ch == "8":
        source = ask("Enter the Source:")
        content = ask("Enter the Content Phrase:")
        return {"bool": {"should": [{"match": {"source": source}}, {"match": {"content": content}}]}}
    if ch == "9":
        query = ask("Enter the Query:")
        return {"match": {"content": {"query": query, "operator": "and"}}}
    if ch == "10":
        query = ask("Enter the Query:")
        return {"match": {"content": {"query": query, "operator": "or"}}}
    return None


def search_query(elastic, ch, index=DEFAULT_INDEX, ask=input):
    query = build_query(ch, ask)
    if query is None:
        return None
    return elastic.search(index=index, query=query, size=N_DOCS)


def relevance_vector(result, n_docs=N_DOCS):
    """Mark returned document ids as relevant (1) in a vector indexed 1..n_docs.

    Note: as in the original, "relevant" means "returned by the query", and the
    returned ids are sorted numerically rather than by score.
    """
    ordered = sorted(int(doc["_id"]) for doc in result["hits"]["hits"])
    vec = np.zeros(n_docs + 1)
    for doc_id in ordered:
        if 1 <= doc_id <= n_docs:
            vec[doc_id] = 1
    return ordered, vec


def mapr(totallist1, orderedsearch, n_docs=N_DOCS):
    """Precision/recall table and averages; arithmetic identical to the original ``mapr``."""
    precision = 0.0
    recall = 0.0
    average_precision = 0.0
    average_recall = 0.0
    c = 0
    print("Document Count\tPrecision\tRecall\tRelevancy")
    for i in range(1, n_docs + 1):
        if totallist1[i] == 1:
            precision += 1.0
            recall += 1.0
            temp = precision / (i + 1)
            average_precision += temp
            temper = recall / n_docs
            temp = round(temp, 5)
            temper = round(temper, 5)
            average_recall += temper
            if orderedsearch[c] == orderedsearch[-1]:
                print(i + 1, temp, temper, orderedsearch[c])
            else:
                print(i + 1, temp, temper, orderedsearch[c])
                c += 1
        else:
            temp = precision / (i + 1)
            temper = recall / n_docs
            average_precision += temp
            average_recall += temper
            print(i + 1, temp, temper, orderedsearch[c] if orderedsearch else 0)
    map_ = average_precision / n_docs
    mar = average_recall / n_docs
    print("Mean Average Precision:", map_)
    print("Mean Average Recall:", mar)
    return map_, mar


MENU = """
1.Searching for word through title ranged using published date:
2.Title and content is asked:
3.Media type and content range of published date:
4.Any title but content should contain start phrase:
5.Title should be exact
6.Content have higher importance than title:
7.Content should begin with a letter:
8.Source and content is asked:
9.Enter 2 keywords, will return if both words appear together in content:
10.Enter 2 keywords, will return if both words appear together or separately in content:"""


def main(argv=None, elastic=None, ask=input):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="http://localhost:9200")
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--data", default="sample-1M.jsonl", help="Signal Media 1M news JSONL file")
    args = parser.parse_args(argv)

    elastic = elastic or connect(args.host)

    if ask("Is the file Indexed in Elastic Search? ").strip().lower() == "no":
        indexing(elastic, args.data, args.index)

    ch = ask("Would you like to query the search engine? press y/n:")
    while ch == "y":
        print(MENU)
        result = search_query(elastic, ask("Enter a choice of query:"), args.index, ask)
        if result is None:
            print("Invalid choice, please enter a number from 1 to 10.")
            continue
        summary(result)
        orderedsearch, totallist1 = relevance_vector(result)
        mapr(totallist1, orderedsearch)
        ch = ask("Would you like to query the search engine? press y/n:")


if __name__ == "__main__":
    main()
