from elasticsearch import Elasticsearch
import sys
from collections import defaultdict, OrderedDict
import operator
import math

es = Elasticsearch()
vocab_size = 178081.0


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
    doc_set = dict()
    page = es.search(
        index=indexName,
        doc_type=type,
        scroll='2m',
        _source_include=['docno', 'doclength'],
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
            doc_set[hit['_source']['docno']] = int(hit['_source']['doclength'])

        print "Scrolling..."
        page = es.scroll(scroll_id=sid, scroll='2m')
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        # print "scroll size: " + str(scroll_size)

    return doc_set


def gettfidf(ts_map, query_text, avg_doclength, doclength):
    score = 0.0
    for term in query_text:
        terms_map = ts_map['terms']
        if term in terms_map:
            tf = float(terms_map[term]['term_freq'])
            df = float(terms_map[term]['doc_freq'])
            okapi_tf = float(tf) / float(tf + 0.5 + 1.5 * (float(doclength) / float(avg_doclength)))
            score += (okapi_tf * math.log(float(84678.0) / df, 10))

    return score


def getokapi(ts_map, query_text, avg_doclength, doclength):
    score = 0.0
    for term in query_text:
        terms_map = ts_map['terms']
        if term in terms_map:
            tf = float(terms_map[term]['term_freq'])
            score += float(tf) / float(tf + 0.5 + 1.5 * (float(doclength) / float(avg_doclength)))

    return score


def getbm25(ts_map, query_text, avg_doclength, doclength):
    score = 0.0
    for term in query_text:
        terms_map = ts_map['terms']
        word_count = query_text.count(term)
        query_factor = float(word_count * 101.0) / float(100.0 + word_count)
        if term in terms_map:
            tf = float(terms_map[term]['term_freq'])
            df = float(terms_map[term]['doc_freq'])
            okapi = float(tf + tf * 1.2) / float(tf + (1.2 * (0.25 + 0.75 * float(doclength / avg_doclength))))
            score += (okapi * math.log(float(84678.0) / (df + 0.5), 10) * query_factor)

    return score


def getunilm(ts_map, query_text, avg_doclength, doclength):
    # 1.0 * size * Math.log10(1.0 / vocabSize)
    score = 1.0 * float(len(query_text)) * math.log(float(1.0) / float(vocab_size), 10)
    for term in query_text:
        terms_map = ts_map['terms']
        if term in terms_map:
            tf = float(terms_map[term]['term_freq'])
            # Math.log10((v.getTf() + 1.0) / (docLength + 198965.0)) - Math.log10(1.0 / 198965.0);
            score += (
                math.log(float(tf + 1.0) / float(doclength + vocab_size), 10) - math.log(float(1.0) / float(vocab_size),
                                                                                         10))

    return score


def getuniJL(ts_map, query_text, avg_doclength, doclength, ttf_map):
    lambda_value = 0.5
    score = 0.0
    for term in query_text:
        tf = 0.0
        terms_map = ts_map['terms']
        if term in terms_map:
            terms_map = ts_map['terms']
            tf = float(terms_map[term]['term_freq'])
        ttf = float(ttf_map[term])
        cal_score = lambda_value * (float(tf) / float(doclength)) + (1.0 - lambda_value) * (
            float(ttf) / float(vocab_size))
        score += math.log(cal_score, 10)

    return score


def getScoreTuple(indexName, docid, query_text, avg_doclength, doclength, word_ttf_map):
    ts = es.termvectors(index=indexName,
                        doc_type='hw1',
                        id=docid,
                        field_statistics=True,
                        fields=['text'],
                        term_statistics=True)

    okapi = getokapi(ts["term_vectors"]["text"], query_text, avg_doclength, doclength)
    tfidf = gettfidf(ts["term_vectors"]["text"], query_text, avg_doclength, doclength)
    bm25 = getbm25(ts["term_vectors"]["text"], query_text, avg_doclength, doclength)
    unilm = getunilm(ts["term_vectors"]["text"], query_text, avg_doclength, doclength)
    uniJL = getuniJL(ts["term_vectors"]["text"], query_text, avg_doclength, doclength, word_ttf_map)

    return (float(okapi), float(tfidf), float(bm25), float(unilm), float(uniJL), doclength)


def write_matrix(query, temp_query_map, test_queries_set):
    if query in test_queries_set:
        with open('test_matrix.txt', 'a+') as f:
            for docId, score_tuple in temp_query_map.iteritems():
                temp_str = []
                temp_str.append(str(query) + ':' + str(docId))
                for index, eval_system_score in enumerate(score_tuple):
                    if index == 0:
                        temp_str.append(str(eval_system_score))
                    else:
                        temp_str.append(str(index) + ":" + str(eval_system_score))

                string_write = ' '.join(temp_str) + '\n'

                f.write(string_write)
    else:
        with open('train_matrix.txt', 'a+') as f:
            for docId, score_tuple in temp_query_map.iteritems():
                temp_str = []
                temp_str.append(str(query) + ':' + str(docId))
                for index, eval_system_score in enumerate(score_tuple):
                    if index == 0:
                        temp_str.append(str(eval_system_score))
                    else:
                        temp_str.append(str(index) + ":" + str(eval_system_score))

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

    return token_list[1:]


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


def getAvgDocLength(param, param1):
    return 441.6


def get_TTF_map(queries_map, indexName, type):
    ttf_map = defaultdict(lambda: 1.0)
    for query_id, query_text in queries_map.iteritems():
        for term in query_text:
            json = es.search(
                index=indexName,
                doc_type=type,
                _source_include=['docno'],
                size=1000,
                body={

                    "query": {
                        "term": {
                            "docno": "AP891231-0047"
                        }
                    },
                    "script_fields": {
                        "ttf": {
                            "script": {
                                "lang": "groovy",
                                "inline": "_index['text'][TERM].ttf()",
                                "params": {
                                    "TERM": term
                                }

                            }
                        }
                    }

                })

            count = int(json['hits']['hits'][0]['fields']['ttf'][0])
            if count == 0:
                ttf_map[term] = 1.0
            else:
                ttf_map[term] = count

    return ttf_map


def write_formatted_matrix(input_file_path, output_file_name):
    f = open(input_file_path, 'r')
    l = f.readlines()
    with open(output_file_name+'.txt', 'a+') as w:
        for line in l:
            try:
                line_array = line.split()
                string_write = ' '.join(line_array[1:]) + '\n'
                w.write(string_write)
            except Exception, e:
                print e
    f.close()


if __name__ == '__main__':
    test_queries_set = set([56, 57, 64, 71, 99])
    avg_doc = getAvgDocLength('ap_dataset', 'hw1')
    queries_map = read_queries('query_desc.51-100.short.txt')
    word_ttf_map = get_TTF_map(queries_map, "ap_dataset", "hw1")
    all_docs = read_all_docs('ap_dataset', 'hw1')
    qrel, num_rel = read_qrel('qrels.adhoc.51-100.AP89.txt')
    for query, docMap in qrel.iteritems():
        if query in queries_map:
            count = 0
            temp_query_map = defaultdict(lambda: ())
            query_text = queries_map[int(query)]
            for docid, relvance in docMap.iteritems():
                all_score_tuple = getScoreTuple('ap_dataset', docid, query_text, avg_doc, all_docs[docid], word_ttf_map)
                temp_query_map[docid] = (relvance,) + all_score_tuple
                count += 1

            if count < 1000:
                for docid in all_docs:
                    if count == 1000:
                        write_matrix(query, temp_query_map, test_queries_set)
                        break

                    if docid not in temp_query_map:
                        all_score_tuple = getScoreTuple('ap_dataset', docid, query_text, avg_doc, all_docs[docid],
                                                        word_ttf_map)
                        temp_query_map[docid] = (0,) + all_score_tuple
                        count += 1

    write_formatted_matrix('train_matrix.txt','train_matrix_formatted')
    write_formatted_matrix('test_matrix.txt','test_matrix_formatted')