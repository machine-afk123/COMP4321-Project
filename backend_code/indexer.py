# from nltk.corpus import stopwords
import nltk
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import word_tokenize
from collections import defaultdict
import requests
from bs4 import BeautifulSoup
import json
import crawler
import db

try:
    nltk.data.find('tokenizers/punkt')
    print("Punkt tokenizer is already downloaded.")
except LookupError:
    print("Downloading punkt tokenizer...")
    nltk.download('punkt')
    print("Punkt tokenizer downloaded successfully.")

def get_stopwords():
    stopword_list = []
    with open("C:\\Users\\Joshua Serrao\\Documents\\GitHub\\COMP4321-Project\\backend_code\\stopwords.txt") as file_obj: # change to path where stopwords.txt is stored if necessary
        for line in file_obj:
            stopword = line.strip()
            stopword_list.append(stopword)

    return stopword_list

def stopword_remover(text):
    stopwords = get_stopwords()
    if type(text) != list:
        tokenized_words = word_tokenize(text)
    else:
        tokenized_words = text
    filtered_words = []
    for word in tokenized_words:
        if word not in stopwords:
            filtered_words.append(word)

    return filtered_words

def stemmer(filtered_words):
    porter_stemmer = PorterStemmer()
    stemmed_words = []
    for word in filtered_words:
        stemmed_word = porter_stemmer.stem(word)
        if len(stemmed_word) == 0:
            continue
        else:
            stemmed_words.append(stemmed_word)

    return stemmed_words

def index(crawler_text):
    filtered_words = stopword_remover(crawler_text)
    preprocessed_words = stemmer(filtered_words)

    return preprocessed_words

def word_dict(words):
    word_dict = defaultdict(lambda: {'frequency': 0, 'positions': []})

    for i, word in enumerate(words):
        word_dict[word]['frequency'] += 1
        word_dict[word]['positions'].append(i)

    return word_dict

def create_inverted_index(forward_index_body, forward_index_title):
    inverted_index_body = {}
    inverted_index_title = {}

    for url, word_dict in forward_index_body.items():
        for word, info in word_dict.items():
            if word not in inverted_index_body:
                inverted_index_body[word] = {}
            if url not in inverted_index_body[word]:
                inverted_index_body[word][url] = []
            inverted_index_body[word][url].append(info['frequency'])
            inverted_index_body[word][url].append(info['positions'])

    for url, word_dict in forward_index_title.items():
        for word, info in word_dict.items():
            if word not in inverted_index_title:
                inverted_index_title[word] = {}
            if url not in inverted_index_title[word]:
                inverted_index_title[word][url] = []
            inverted_index_title[word][url].append(info['frequency'])
            inverted_index_title[word][url].append(info['positions'])

    return inverted_index_body, inverted_index_title

def create_index(conn, cursor):
    # conn, cursor = db.init_connection()
    try:
        base_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
        # base_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/Movie.htm"
        num_pages = 300 # MODIFY THIS PARAMETER TO CHANGE THE num_pages
        # num_pages = 1
        web_crawler = crawler.Crawler(base_url, num_pages)
        
        extracted_links = web_crawler.bfs_extract()
        extracted_links = sorted(extracted_links)
        text = ""
        pages = {}
        for page_url in extracted_links:
            response = requests.get(page_url)
            response.raise_for_status()
            page_size = len(response.content) 
            beautiful_soup = BeautifulSoup(response.content, "html.parser")
            title, body = web_crawler.get_content(beautiful_soup)
            pages[page_url] = {
                'page_size': page_size,
                'last_modified': web_crawler.get_last_modified_date(page_url),
                'title': title,
                'body': body,
                'child_links': web_crawler.crawl(page_url)
            }
            text += title + " " + body + " "
        
        non_stemmed_words = stopword_remover(text)
        db.populate_mapping(conn, cursor, non_stemmed_words, "non_stemmed_mapping")

        
        stemmed_words = stemmer(non_stemmed_words)
        db.populate_mapping(conn, cursor, stemmed_words, "stemmed_mapping")

        db.populate_mapping(conn, cursor, pages.keys(), "page_mapping")

        db.populate_pageinfo(conn, cursor, pages)

        forward_index_body = {}
        forward_index_title = {}

        for url, page_info in pages.items():
            processed_body = index(page_info['body'])
            processed_title = index(page_info['title'])
            body_dict = word_dict(processed_body)
            title_dict = word_dict(processed_title)

            forward_index_body[url] = {}
            for word, info in body_dict.items():
                db.populate_forward_index(conn, cursor, url, word, info['frequency'], info['positions'], "forwardIndex_body")
                forward_index_body[url][word] = {
                    'frequency': info['frequency'], 
                    'positions': info['positions']
                }

            forward_index_title[url] = {}
            for word, info in title_dict.items():
                db.populate_forward_index(conn, cursor, url, word, info['frequency'], info['positions'], "forwardIndex_title")
                forward_index_title[url][word] = {
                    'frequency': info['frequency'], 
                    'positions': info['positions']
                }
            
        inverted_index_body, inverted_index_title = create_inverted_index(forward_index_body, forward_index_title)
        for word in inverted_index_body:
            serialized_body_json =  json.dumps(inverted_index_body[word])
            db.populate_inverted_index(conn, cursor, word, serialized_body_json, "invertedIndex_body")
        for word in inverted_index_title:
            serialized_title_json = json.dumps(inverted_index_title[word])
            db.populate_inverted_index(conn, cursor, word, serialized_title_json, "invertedIndex_title")
    finally:
        db.close_connection(conn)
        web_crawler.close_connection()