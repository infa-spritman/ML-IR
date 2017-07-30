from elasticsearch import Elasticsearch
import sys
from collections import defaultdict, OrderedDict
import operator
import math

es = Elasticsearch()


def read_qrel(file_qrel):
    temp_qrel = defaultdict(lambda: defaultdict(lambda: 0))
    temp_num_rel = defaultdict(lambda: 0)

    f = open(file_qrel, 'r')
    l = f.readlines()
    for line in l:
        try:
            topic, dummy, doc_id, rel = line.split()
            rel = int(rel)
            temp_qrel[int(topic)][doc_id] = rel
            if rel > 0:
                temp_num_rel[int(topic)] += 1
            else:
                temp_num_rel[int(topic)] += 0
        except Exception, e:
            print e
    f.close()
    return temp_qrel, temp_num_rel


# Citation : https://gist.github.com/drorata/146ce50807d16fd4a6aa (  for scroll )

def read_all_docs(indexName, type):
    doc_set = set()
    page = es.search(
        index=indexName,
        doc_type=type,
        scroll='2m',
        _source_include=['docno'],
        size=1000,
        body={
            "query": {
                "match_all": {}
            }
        })
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    print scroll_size
    # Start scrolling
    while (scroll_size > 0):

        for hit in page['hits']['hits']:
            doc_set.add(str(hit['_source']['docno']))

        print "Scrolling..."
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        # print "scroll size: " + str(scroll_size)

    return doc_set


def getScoreTuple(docid, query_text):
    return (1.0,1.0,10.0,11.0,12.0)


def write_matrix(query, temp_query_map, test_queries_set):
    if query in test_queries_set:
        with open('test_matrix.txt', 'a+') as f:
            for docId, score_tuple in temp_query_map.iteritems():
                temp_str = []
                temp_str.append(str(query) + '-' + str(docId))
                for index, eval_system_score in enumerate(score_tuple):
                    temp_str.append(str(eval_system_score))

                string_write = ' '.join(temp_str) + '\n'

                f.write(string_write)
    else:
        with open('train_matrix.txt', 'a+') as f:
            for docId, score_tuple in temp_query_map.iteritems():
                temp_str = []
                temp_str.append(str(query) + '-' + str(docId))
                for index, eval_system_score in enumerate(score_tuple):
                    temp_str.append(str(eval_system_score))

                string_write = ' '.join(temp_str) + '\n'

                f.write(string_write)


def analysed_query(indexName, line):
    token_list = []
    json = es.indices.analyze(index=indexName, body={

        "analyzer": "my_english",
        "text": line

    })

    for tok in json['tokens']:
         token_list.append(tok['token'])


    return token_list


def read_queries(file_queries):
    temp_map = dict()

    f = open(file_queries, 'r')
    l = f.readlines()
    for line in l:
        try:
            line_array = line.split()
            formatted_query_terms = analysed_query('ap_dataset', line)
            temp_map[int(line_array[0].replace('.', ''))] = formatted_query_terms
        except Exception, e:
            print e
    f.close()
    return temp_map


if __name__ == '__main__':
    test_queries_set = set([56, 57, 64, 71, 99])
    queries_map = read_queries('query_desc.51-100.short.txt')
    all_docs = read_all_docs('ap_dataset', 'hw1')
    qrel, num_rel = read_qrel('qrels.adhoc.51-100.AP89.txt')
    for query, docMap in qrel.iteritems():
        if query in queries_map:
            count = 0
            temp_query_map = defaultdict(lambda: ())
            query_text = queries_map[int(query)]
            for docid, relvance in docMap.iteritems():
                all_score_tuple = getScoreTuple(docid, query_text)
                temp_query_map[docid] = all_score_tuple + (relvance,)
                count += 1

            if count < 1000:
                for docid in all_docs:
                    if count == 1000:
                        write_matrix(query, temp_query_map, test_queries_set)
                        break

                    if docid not in temp_query_map:
                        all_score_tuple = getScoreTuple(docid, query_text)
                        temp_query_map[docid] = all_score_tuple + (0,)
                        count += 1
