import numpy as np
import json
import json_lines
from nltk.stem.snowball import SnowballStemmer
from elasticsearch import Elasticsearch

elastic = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def indexing():
    with open('sample-1M.jsonl', 'rb') as f:
        i = 1
        for data in json_lines.reader(f):
            if(i >= 8000):
                continue
            else:
                data = json.dumps(data)
                decoded = json.loads(data)
                elastic.index(index = 'news_article_dataset', doc_type = 'articles', id = i, body = decoded)
            i = i + 1

def summary(result):
    for data in result['hits']['hits']:
        print("Document Index is:", data['_id'])
        print("Document Search Score is:", data['_score'])
        print("Document Media Type:", data['_source']['media-type'])
        print("Document Title:", data['_source']['title'])
        print("Source:", data['_source']['source'])
        print("Published on:", data['_source']['published'])
        print("Document Content:", data['_source']['content'],'\n')

def search_by_index(ids):
    result = elastic.search(index = 'news_article_dataset', doc_type = 'articles', id = ids, body = decoded)
    return result

def search_query(ch):    
    if ch == '1':
        title = input('Enter the Title:')
        start = input('Enter the Published Start Date (YYYY/MM/DD):')
        end = input('Enter the Published End Date (YYYY/MM/DD):')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"bool":{"should":[{"match":{"title":title}},{"range":{"published":{"gte": start, "lte": end, "format": "yyyy/MM/dd||yyyy"}}}]}}}, size = 8000)
    if ch == '2':
        title = input('Enter the Title:')
        content = input('Enter the Content Phrase:')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"bool":{"should":[{"match":{"title":title}},{"match":{"content":content}}]}}}, size = 8000)
    if ch == '3':
        media = input('Enter the Media-type:')
        content = input('Enter the Content Phrase:')
        start = input('Enter the Published Start Date (YYYY/MM/DD):')
        end = input('Enter the Published End Date (YYYY/MM/DD):')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"bool":{"should":[{"match":{"media-type":media}},{"match":{"content":content}},{"range":{"published":{"gte": start, "lte": end, "format": "yyyy/MM/dd||yyyy"}}}]}}}, size = 8000)
    if ch == '4':
        title = input('Enter the Title:')
        content = input('Enter the Content Start Phrase:')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"bool":{"should":[{"match":{"title":title}},{"match_phrase_prefix":{"content":content}}]}}}, size = 8000)
    if ch == '5':
        title = input('Enter the Exact Title:')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"match_phrase":{"title":title}}}, size = 8000)
    if ch == '6':
        keyword = input('Enter Query Keyword:')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query": {"multi_match":{"query":keyword,"fields": ["content^2", "title"]}}}, size = 8000)
    if ch == '7':
        content = input("Enter the Letter:")
        content = content + "*"
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"wildcard": {"content": content}}}, size = 8000)
    if ch == '8':
        source = input('Enter the Source:')
        content = input('Enter the Content Phrase:') 
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"bool":{"should":[{"match":{"source":source}},{"match":{"content":content}}]}}}, size = 8000)
    if ch == '9': 
        query = input('Enter the Query:')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"match":{"content":{"query":query,"operator":"and"}}}}, size = 8000)
    if ch == '10':
        query = input('Enter the Query:')
        result = elastic.search(index = "news_article_dataset", doc_type = "articles", body = {"query":{"match":{"content":{"query":query,"operator":"or"}}}}, size = 8000)
    return result

def mapr(precision, recall):
    average_precision = 0.0
    average_recall = 0.0
    c = 0
    print('Document Count\tPrecision\tRecall\tRelevancy')
    for i in range(1, 8001):
        if totallist1[i] == 1:
            precision = precision + 1.0
            recall = recall + 1.0
            temp = precision /(i+1)
            average_precision = average_precision + temp
            temper = recall / 8000
            temp = round(temp, 5)
            temper = round(temper, 5)
            average_recall = average_recall + temper
            if(orderedsearch[c] == orderedsearch[-1]):
                print(i+1, temp, temper, orderedsearch[c])
            elif(orderedsearch == []):
                print(i+1, temp, temper, 0)
            else:
                print(i+1, temp, temper, orderedsearch[c])
                c = c + 1
        else:
            temp = precision /(i+1)
            temper = recall / 8000
            average_precision = average_precision + temp
            average_recall = average_recall + temper
            if(orderedsearch == []):
                print(i+1, temp, temper, 0)
            else:
                print(i+1, temp, temper, orderedsearch[c])
                
    print('Mean Average Precision:',(average_precision/8000))        
    print('Mean Average Recall:',(average_recall/8000))

index = input('Is the file Indexed in Elastic Search? ')
if(index == 'no' or index == 'No' or index == 'NO'):                  
    indexing()       

ch = input('Would you like to query the search engine? press y/n:')
while( ch == "y"):
    print("\n1.Searching for word through title ranged using published date:")
    print("2.Title and content is asked:")
    print("3.Media type and content range of published date:")
    print("4.Any title but content should contain start phrase:")
    print("5.Title should be exact")
    print("6.Content have higher importance than title:")
    print('7.Content should begin with a letter:')
    print("8.Source and content is asked:")
    print("9.Enter 2 keywords, will return if both words appear togather in content:")                  
    print("10.Enter 2 keywords, will return if both words appear together or seperately in content:")
    ch = input('Enter a choice of query:')
    result = search_query(ch)
    summary(result)
                      
    numberofhits1 = []
    for doc in result['hits']['hits']:
        numberofhits1.append(doc['_id'])

    for i in range(len(numberofhits1)):
      numberofhits1[i] = int(numberofhits1[i])

    numberofhits1.sort()
    orderedsearch = numberofhits1
    totallist = np.zeros(8001)
    totallist1 = np.zeros(8001)
    for i in range(8000):
        for j in range(len(orderedsearch)):
            if orderedsearch[j] == i+1:
                totallist1[i+1] = 1
            
    precision = 0.0
    recall = 0.0
    mapr(precision,recall)
    ch = input('Would you like to query the search engine? press y/n:')
