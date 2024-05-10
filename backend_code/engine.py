import json
import db
import sqlite3
import indexer
import math
from collections import defaultdict
from sklearn.metrics.pairwise import cosine_similarity
from nltk.stem import PorterStemmer
import numpy


db_conn = sqlite3.connect('C:\\Users\\Joshua Serrao\\Documents\\GitHub\\COMP4321-Project\\web_crawler.db')
c = db_conn.cursor()   

def phrase_stemmer(phrase):
    """Stemmer for phrases"""
    filtered_words = indexer.stopword_remover(phrase)
    stemmed_words = indexer.stemmer(filtered_words)
 
    return stemmed_words

def get_page_id_mappings():
    c.execute("SELECT page_id, url FROM page_mapping")
    page_rows = c.fetchall()
    page_id_mapping = {}

    for row in page_rows:
        page_id_mapping[row[0]] = row[1]

    return page_id_mapping # dict -> {page_id : url}

def get_word_id_mappings():
    c.execute("SELECT word_id, word FROM stemmed_mapping")
    rows = c.fetchall()

    word_id_mapping = {}

    for row in rows:
        word_id_mapping[row[0]] = row[1]

    return word_id_mapping # dict -> {word_id : word}


# def phrase_stemmer(text):
#     stopwords = set(line.strip() for line in open('C:\\Users\\Joshua Serrao\\Documents\\GitHub\\COMP4321-Project\\backend_code\\stopwords.txt')) 
#     stemmer = PorterStemmer() 

#     stemmed_text = [] 

#     for phrase in text: 
#         stemmed_phrase = ""
#         for word in phrase.split():
#             stemmed_word = stemmer.stem(word) 
#             if stemmed_word not in stopwords: 
#                 stemmed_phrase += (stemmed_word + " ")
#         if len(stemmed_phrase) > 0 and stemmed_phrase[-1] == " ":
#             stemmed_phrase = stemmed_phrase[:-1] 
#         stemmed_text.append(stemmed_phrase)

#     return stemmed_text  

def get_page_body_size_map():
    page_body_sizes = {}

    page_mappings = get_page_id_mappings()

    c.execute("SELECT page_id, body FROM page_info")
    rows = c.fetchall()

    for row in rows:
        page_body_sizes[page_mappings[row[0]]] = len(row[1].split())

    return page_body_sizes # dict -> {page_url : num_words}

def get_page_title_size_map():
    page_title_sizes = {}
    c.execute("SELECT page_id, title FROM page_info")
    rows = c.fetchall()

    page_mappings = get_page_id_mappings()

    for row in rows:
        page_title_sizes[page_mappings[row[0]]] = len(row[1].split())

    return page_title_sizes # dict -> {page_url : num_words}

# def get_max_tf_body():
#     max_tf_map = {}
#     c.execute("SELECT word_id, pages_freq FROM invertedinverted_index_body")
#     rows_inv_inverted_index_body = c.fetchall()


#     for row in rows_inv_inverted_index_body:
#         c.execute("SELECT word FROM stemmed_mapping WHERE word_id=?", (row[0],))
#         word = c.fetchone()[0]
#         pages_freq = json.loads(row[1])
#         max_tf_map[word] = 0
#         for doc, info in pages_freq.items():
#             tf = info[0]
#             max_tf_map[word] = max(tf, max_tf_map[word])

#     return max_tf_map

# def get_max_tf_title():
#     max_tf_map = {}
#     c.execute("SELECT word_id, pages_freq FROM invertedinverted_index_title")
#     rows_inv_inverted_index_body = c.fetchall()


#     for row in rows_inv_inverted_index_body:
#         c.execute("SELECT word FROM stemmed_mapping WHERE word_id=?", (row[0],))
#         word = c.fetchone()[0]
#         pages_freq = json.loads(row[1])
#         max_tf_map[word] = 0
#         for doc, info in pages_freq.items():
#             tf = info[0]
#             max_tf_map[word] = max(tf, max_tf_map[word])

#     return max_tf_map


def get_df_body():
    df_map = {}
    pages_freq_map = {}
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_body")
    rows = c.fetchall()

    word_id_mappings = get_word_id_mappings()

    for row in rows:
        # pages_freq = {}
        pages_freq = json.loads(row[1])
        df = len(pages_freq.keys())
        word = word_id_mappings[row[0]]     

        df_map[word] = df
        pages_freq_map[word] = pages_freq

    return df_map, pages_freq_map # df_map -> {word : num urls}, pages_freq_map ->{word : {url : [freq, [positions]]}}


def get_df_title():
    df_map = {}
    pages_freq_map = {}
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_title")
    rows = c.fetchall()

    word_id_mappings = get_word_id_mappings()

    for row in rows:
        # pages_freq = {}
        pages_freq = json.loads(row[1])
        df = len(pages_freq.keys())
        word = word_id_mappings[row[0]]

        df_map[word] = df
        pages_freq_map[word] = pages_freq

    return df_map, pages_freq_map # df_map -> {word : num urls}, pages_freq_map ->{word : {url : [freq, [positions]]}}

def get_tf_idf_score_body():
    tf_idf_map = defaultdict(lambda: defaultdict(float))
    # get the df
    df_map, pages_freq_map = get_df_body()

    c.execute("SELECT COUNT(*) FROM page_mapping")
    n = c.fetchone()[0]

    # get the max tf
    # max_tf_map = get_max_tf_body()
    page_sizes = get_page_body_size_map()

    for word, pages_freq in pages_freq_map.items():
        for url, word_data in pages_freq.items():
            tf = word_data[0]
            weight = (0.5 + (0.5 * (tf / page_sizes[url]))) * math.log2(1 + (n/df_map[word]))
            tf_idf_map[word][url] = weight

    return tf_idf_map # tf_idf_map -> {word : {url : weight}}

def get_tf_idf_score_title():
    tf_idf_map = defaultdict(lambda: defaultdict(float))
    # get the df
    df_map, pages_freq_map = get_df_title()

    c.execute("SELECT COUNT(*) FROM page_mapping")
    n = c.fetchone()[0]

    # get the max tf
    # max_tf_map = get_max_tf_title()
    page_sizes = get_page_title_size_map()

    for word, pages_freq in pages_freq_map.items():
        for url, word_data in pages_freq.items():
            tf = word_data[0]
            weight = (0.5 + (0.5 * (tf / page_sizes[url]))) * math.log2(1 + (n/df_map[word]))
            tf_idf_map[word][url] = weight

    return tf_idf_map  # tf_idf_map -> {word : {url : weight}}


def get_phrase_body_tf_idf(query_phrase):
    # return the frequency of the phrases in the doc
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_body")
    rows = c.fetchall()

    word_mapping = get_word_id_mappings()

    page_id_mappings = get_page_id_mappings()

    phrase_frequency = {}

    tf_idf_map = defaultdict(lambda: defaultdict(float))

    inverted_index_body = {}

    for row in rows:
        word_id = row[0]
        word = word_mapping[word_id]
        inverted_index_body[word] = {}
        inverted_index_body[word] = json.loads(row[1]) # inverted_index_body -> {word : {url: [freq, [positions]]}}

    for phrase in query_phrase:
        phrase_frequency[phrase] = {}
        for page_id in page_id_mappings.keys():
            prev_positions = []
            cur_positions = []
            containsPhrase = True
            
            for word in phrase.split():
                if word not in inverted_index_body.keys():
                    containsPhrase = False
                    break
                if inverted_index_body[word].get(page_id_mappings[page_id], 0) == 0:
                    containsPhrase = False
                    break

                cur_positions = inverted_index_body[word][page_id_mappings[page_id]][1]
                
                if word == phrase.split()[0]:
                    prev_positions = cur_positions
                else:
                    temp_positions = []
                    for prev_position in prev_positions:
                        if prev_position + 1 in cur_positions:
                            temp_positions.append(prev_position + 1)
                    prev_positions = temp_positions

                if len(prev_positions) == 0:
                    containsPhrase = False
                    break

            if containsPhrase:
                freq = len(prev_positions) 
            else:
                freq = 0    

            phrase_frequency[phrase][page_id_mappings[page_id]] = freq # phrase_frequency -> {phrase : {url : freq }}

    page_sizes = get_page_title_size_map()

    for phrase, pages_freq in phrase_frequency.items():
        for url, phrase_freq in pages_freq.items():
            tf = phrase_freq
            weight = (0.5 + (0.5 * (tf / page_sizes[url]))) * math.log2(1 + (len(page_id_mappings.keys())/len(phrase_frequency[phrase].keys())))
            tf_idf_map[phrase][url] = weight

    return phrase_frequency, tf_idf_map # phrase_frequency -> {phrase : {url : freq }}, tf_idf_map -> {phrase : {url : weight}}
        

def get_phrase_title_tf_idf(query_phrase):
    # return the frequency of the phrases in the doc
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_title")
    rows = c.fetchall()

    word_mapping = get_word_id_mappings()

    page_id_mappings = get_page_id_mappings()
         
    phrase_frequency = {}

    tf_idf_map = defaultdict(lambda: defaultdict(float))

    inverted_index_title = {}

    for row in rows:
        word_id = row[0]
        word = word_mapping[word_id]
        inverted_index_title[word] = {}
        inverted_index_title[word] = json.loads(row[1]) # inverted_index_title -> {word : {url: [freq, [positions]]}}

    for phrase in query_phrase:
        phrase_frequency[phrase] = {}
        for page_id in page_id_mappings.keys():
            prev_positions = []
            cur_positions = []
            containsPhrase = True
            
            for word in phrase.split():
                if word not in inverted_index_title.keys():
                    containsPhrase = False
                    break
                if inverted_index_title[word].get(page_id_mappings[page_id], 0) == 0:
                    containsPhrase = False
                    break

                cur_positions = inverted_index_title[word][page_id_mappings[page_id]][1]
                
                if word == phrase.split()[0]:
                    prev_positions = cur_positions
                else:
                    temp_positions = []
                    for prev_position in prev_positions:
                        if prev_position + 1 in cur_positions:
                            temp_positions.append(prev_position + 1)
                    prev_positions = temp_positions

                if len(prev_positions) == 0:
                    containsPhrase = False
                    break

            if containsPhrase:
                freq = len(prev_positions) 
            else:
                freq = 0    

            phrase_frequency[phrase][page_id_mappings[page_id]] = freq      

    page_sizes = get_page_title_size_map()

    for phrase, pages_freq in phrase_frequency.items():
        for url, phrase_freq in pages_freq.items():
            tf = phrase_freq
            weight = (0.5 + (0.5 * (tf / page_sizes[url]))) * math.log2(1 + (len(page_id_mappings.keys())/len(phrase_frequency[phrase].keys())))
            tf_idf_map[phrase][url] = weight

    return phrase_frequency, tf_idf_map # phrase_frequency -> {phrase : {url : freq }}, tf_idf_map -> {phrase : {url : weight}}

def calculate_similarity(query_term, query_phrase, body_weights, title_weights):
    # tf_idf_weights: a dictionary. key: term, value: dictionary,  {page_id: tf-idf_weight}

    query_term = indexer.index(query_term) 
    query_phrase = phrase_stemmer(query_phrase)

    all_terms = set(body_weights.keys()).union(set(title_weights.keys())).union(set(query_term).union(set(query_phrase)))

    query_vector = {term: 0 for term in all_terms} # query_vector -> {term : isquery(0/1)}
    for term in query_term + list(query_phrase):
        query_vector[term] = 1
    
    page_id_mappings = get_page_id_mappings()

    # doc_vectors = [{term: 0 for term in all_terms} for i in page_id_mappings.keys()] # doc_vectors -> {page_id : {word : weight}}
    doc_vectors = defaultdict(lambda: defaultdict(float))
    for i in page_id_mappings.keys():
        doc_vectors[i] = {}
        for term in all_terms:
            doc_vectors[i][term] = 0
    # 1. check if doc with each of the phrases

    # Calculate the total frequency of a certain phrase in every doc and use it to calculate weight
    phrase_freq_body, phrase_weight_body = get_phrase_body_tf_idf(query_phrase)
    phrase_freq_title, phrase_weight_title = get_phrase_title_tf_idf(query_phrase)

    for term in all_terms:
        for page_id in page_id_mappings.keys():
            # 1. put phrase into the vector
            if term in query_phrase: # doc_score = 0 if phrase_weight == 0
                if page_id in phrase_weight_body[term].keys():
                    doc_vectors[page_id][term] += phrase_weight_body[term][page_id_mappings[page_id]] * 2
                if page_id in phrase_weight_title[term].keys():
                    doc_vectors[page_id][term] += phrase_weight_title[term][page_id_mappings[page_id]] * 3
            # 2. put body term into the vector
            elif term in body_weights.keys() and page_id_mappings[page_id] in body_weights[term].keys():
                weight_dict = body_weights[term]
                doc_vectors[page_id][term] += weight_dict[page_id_mappings[page_id]]
            # 3. put title term into the vector
            elif term in title_weights.keys() and page_id_mappings[page_id] in title_weights[term].keys():
                weight_dict = title_weights[term]
                doc_vectors[page_id][term] += weight_dict[page_id_mappings[page_id]] * 2

    # Convert query_vector and doc_vectors to matrices
    query_matrix = [list(query_vector.values())] # list of bool values for all terms (0/1)
    # doc_matrix = [list(doc_vector.values()) for doc_vector in doc_vectors.items()] # list with length = number of pages, containing lists of weights for each word in that document

    doc_matrix = {} # {page_id : []}
    for page_id, term_info in doc_vectors.items():
        weights = []
        for word, weight in term_info.items():
            weights.append(weight)
        doc_matrix[page_id] = weights
            
    # Calculate cosine similarity
    docs_similarity = {}

    for page_id in page_id_mappings.keys():
        doc_vector = doc_matrix[page_id]
        doc_vector = numpy.array(doc_vector).reshape(1, -1)
        score = cosine_similarity(query_matrix, doc_vector)[0][0]
        score = round(score, 2)
        docs_similarity[page_id] = score

    updated_docs_similarity = {}

    for query in query_phrase:
        for page_id, score in docs_similarity.items():
            if phrase_freq_body[query][page_id_mappings[page_id]] != 0 or phrase_freq_title[query][page_id_mappings[page_id]] != 0:
                updated_docs_similarity[page_id] = score

    docs_similarity = updated_docs_similarity
        
    return docs_similarity


def retrieval_function(query_term, query_phrase):
    # FAVOR: a constant to boost the rank of a page if there is a match in the title
    # the weights are nested dictionary containing weights
    # outer dictionary - key: term  value: inner dictionary
    # inner dictionary - key: page_id value: tfxidf weight
    body_weights = get_tf_idf_score_body()
    title_weights = get_tf_idf_score_title()

    # docs_similarity: a list of tuples - (page_id, cosine similarity)
    docs_similarity = calculate_similarity(query_term, query_phrase, body_weights, title_weights)
    print("docs_similarity: ", docs_similarity)
    result = sorted(docs_similarity, key=lambda x: x[1], reverse=True)
        
    if(len(result) == 0):
        return 0
    elif len(result) >= 50:
        result = result[:50]
    
    return result