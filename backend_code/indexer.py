# from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import word_tokenize
from collections import defaultdict
import requests
from bs4 import BeautifulSoup
import json
import crawler
import db

# PROCESS:
# 1. Remove stopwords
# 2. Transform words into stems
# 3. Insert stems into two inverted files.

def get_stopwords():
    stopword_list = []
    with open("COMP4321-Project\\backend_code\\stopwords.txt") as file_obj:
        stopwords = file_obj.readline()

    for word in stopwords:
        stopword_list.append(word.strip())

    return stopword_list

def stopword_remover(text):
    # TODO: Remove stopwords
    stopwords = get_stopwords()
    tokenized_words = word_tokenize(text)
    filtered_words = []
    for word in tokenized_words:
        if word not in stopwords:
            filtered_words.append(word)

    return filtered_words

def stemmer(filtered_words):
    # TODO: Stem the filtered words
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
    # apply stopword removal and stemming
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
    inverted_index_body = defaultdict(lambda: defaultdict(list))
    inverted_index_title = defaultdict(lambda: defaultdict(list))

    for url, word_dict in forward_index_body.items():
        for word, info in word_dict.items():
            inverted_index_body[word][url].append(info['frequency'])
            inverted_index_body[word][url].append(info['positions'])

    for url, word_dict in forward_index_title.items():
        for word, info in word_dict.items():
            inverted_index_title[word][url].append(info['frequency'])
            inverted_index_title[word][url].append(info['positions'])

    return inverted_index_body, inverted_index_title

def create_index():
    # page_mapping - populate_mapping(attribute, "page_mapping") - one by one
    # stemmed_mapping - populate_mapping(attribute, "stemmed_mapping") - one by one
    # non_stemmed_mapping - populate_mapping(attribute, "non_stemmed_mapping") - one by one

    # page_info - populate_pageinfo(pages) - dictionary of dictionaries
    # pages = {
    #     'page1': {
    #         'page_size': 200,
    #         'last_modified': '2022-01-01',
    #         'title': 'Page 1',
    #         'body': 'This is page 1',
    #         'child_links': ['url2', 'url3']
    #     },
    #     'page2': {
    #         'page_size': 100,
    #         'last_modified': '2022-01-02',
    #         'title': 'Page 2',
    #         'body': 'This is page 2',
    #         'child_links': ['url1']
    #     }
    # }
    
    # forwardIndex_body - populate_forward_index(page_id, word_id, frequency, positions, "forwardIndex_body") - one by one
    # forwardIndex_title - populate_forward_index(page_id, word_id, frequency, positions, "forwardIndex_title") - one by one

    # invertedIndex_body - populate_inverted_index(word_id, page_body_freq, "invertedIndex_body") - one by one
    # invertedIndex_title - populate_inverted_index(word_id, page_title_freq, "invertedIndex_title") - one by one
    # word = {
    #     'word1': {
    #         'page1': [3, [1, 4, 15]],
    #         'page2': [2, [21, 37]],
    #     },
    #     'word2': {
    #         'page1': [1, [3]],
    #         'page2': [2, [5, 11]],
    #     }
    # }
    # page_freq = json.dumps(for each inner dictionary within the word dictionary)

    base_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm"
    num_pages = 30
    web_crawler = crawler.Crawler(base_url, num_pages)
    
    extracted_links = web_crawler.bfs_extract()
    extracted_links = sorted(extracted_links)
    text = ""
    pages = {}
    for page_url in extracted_links:
        response = requests.get(page_url) # make get request to link to get the data.
        response.raise_for_status()
        page_size = len(response.content) # Does this work everytime?
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
    for word in non_stemmed_words:
        db.populate_mapping(word, "non_stemmed_mapping")
    
    stemmed_words = stemmer(non_stemmed_words)
    for word in stemmed_words:
        db.populate_mapping(word, "stemmed_mapping")

    for url in pages:
        db.populate_mapping(url, "page_mapping")

    db.populate_pageinfo(pages)

    forward_index_body = {}
    forward_index_title = {}

    for url, page_info in pages:
        processed_body = index(page_info['body'])
        processed_title = index(page_info['title'])
        body_dict = word_dict(processed_body)
        title_dict = word_dict(processed_title)
        for word, info in body_dict.items():
            db.populate_forward_index(url, word, info['frequency'], info['positions'], "forwardIndex_body")
            forward_index_body[url][word] = {
                'frequency': info['frequency'], 
                'positions': info['positions']
            }
        for word, info in title_dict.items():
            db.populate_forward_index(url, word, info['frequency'], info['positions'], "forwardIndex_title")
            forward_index_title[url][word] = {
                'frequency': info['frequency'], 
                'positions': info['positions']
            }
        
    inverted_index_body, inverted_index_title = create_inverted_index(forward_index_body, forward_index_title)
    for word in inverted_index_body:
        db.populate_inverted_index(word, json.dumps(inverted_index_body[word]), "invertedIndex_body")
    for word in inverted_index_title:
        db.populate_inverted_index(word, json.dumps(inverted_index_title[word]), "invertedIndex_title")
    
         

    



