from elasticsearch import Elasticsearch
import sys
from collections import defaultdict, OrderedDict
import operator
import math

es = Elasticsearch()


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


def writeRankedList(resultDict, topicID):
    rank = 1
    with open('result_1000.txt', 'a+') as f:
        for docId, score in resultDict.iteritems():
            f.write(str(topicID) + ' Q0 ' + docId + ' ' + str(rank) + ' ' + str(score) + ' Exp\n')
            rank += 1


def readProb(simple_test, prob_test):
    temp_prob_map = defaultdict(lambda: defaultdict(lambda: 0))
    f_simple = open(simple_test, 'r')
    l_simple = f_simple.readlines()

    f_prob = open(prob_test, 'r')
    l_prob = f_prob.readlines()

    for i in range(0,len(l_prob)):
        l_simple_array = l_simple[i].split()
        l_prob_array = l_prob[i].split()
        first_element_list = l_simple_array[0].split(':')
        queryid = int(first_element_list[0])
        doc_id = str(first_element_list[1])
        temp_prob_map[queryid][doc_id] = float(l_prob_array[2])

    return temp_prob_map



if __name__ == '__main__':
    queries_map = read_queries('query_desc.51-100.short.txt')
    prob_map = readProb('train_matrix.txt','prob_train.txt')
    for queryID in queries_map:
        print queryID
        result = prob_map[int(queryID)]
        writeRankedList(OrderedDict(sorted(result.iteritems(), key=operator.itemgetter(1), reverse=True)), queryID)
