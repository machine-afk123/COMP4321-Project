# from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import word_tokenize
import crawler

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
    # TODO: Stem the words
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
    # TODO: Apply stopword_remover, stemmer
    # apply stopword removal
    filtered_words = stopword_remover(crawler_text)
    preprocessed_words = stemmer(filtered_words)

    return preprocessed_words

def create_index():
    # TODO: Need to somehow link this to the database
    web_crawler = crawler.Crawler()
    crawled_result = web_crawler.get_content()

