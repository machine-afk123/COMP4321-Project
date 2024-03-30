# from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import word_tokenize
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
    with open("stopwords.txt") as file_obj:
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
    # pages = {
    #     'page1': [3, [1, 4, 15]],
    #     'page2': [2, [21, 37]],
    # }
    # page_body_freq = json.dumps(pages)

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
    for word in non_stemmed_words:
        db.populate_mapping(word, "non_stemmed_mapping")
    
    stemmed_words = stemmer(non_stemmed_words)
    for word in stemmed_words:
        db.populate_mapping(word, "stemmed_mapping")

    for url in pages:
        db.populate_mapping(url, "page_mapping")

    db.populate_pageinfo(pages)

    
         

    



