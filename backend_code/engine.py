import json
import db
import sqlite3
import indexer
import math
from collections import defaultdict

db_conn = sqlite3.connect('web_crawler.db', check_same_thread=False)
c = db_conn.cursor()   

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

def get_page_info(page_ids):
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_body")
    rows_inv_index_body = c.fetchall()
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_title")
    rows_inv_index_title = c.fetchall()
    c.execute("SELECT page_id, page_size, last_modified, title, child_links FROM page_info")
    rows_page_info = c.fetchall()
    page_mappings = get_page_id_mappings()

    word_mappings = get_word_id_mappings()

    page_info_dict = {}

    for row in rows_page_info:
        page_info_dict[row[0]] = [row[1], row[2], row[3], row[4].split(',')]

    result_dict = {} # result_dict -> {page_id : title, url, last_modified, page_size, {keyword : freq}, [parent_links], [child_links]}

    for page_id in page_ids: 
        keyword_freq_map = {}

        for row in rows_inv_index_body:
            pages_freq = json.loads(row[1])
            for page, info in pages_freq.items():
                if page == page_mappings[page_id]:
                    freq = info[0]
                    word = word_mappings[row[0]]
                    keyword_freq_map[word] = freq
                    
        for row in rows_inv_index_title:
            pages_freq = json.loads(row[1])
            for page, info in pages_freq.items():
                if page == page_mappings[page_id]:
                    freq = info[0]
                    word = word_mappings[row[0]]
                    if word not in keyword_freq_map.keys():
                        keyword_freq_map[word] = freq
                    else:
                        keyword_freq_map[word] += freq
        
        keyword_freq_map = {k: v for k, v in sorted(keyword_freq_map.items(), key=lambda item: item[1], reverse=True)}
        keyword_freq_map = dict(list(keyword_freq_map.items())[:5])

        parent_links = []

        for key, value in page_info_dict.items():
            if page_mappings[page_id] in value[3]:
                parent_links.append(page_mappings[key])

        result_dict[page_id] = {
            'title': page_info_dict[page_id][2],  
            'url': page_mappings[page_id], 
            'last_modified': page_info_dict[page_id][1],  
            'page_size': page_info_dict[page_id][0],  
            'keywords': keyword_freq_map,  
            'parent_links': parent_links[:10],  
            'child_links': page_info_dict[page_id][3][:10]  
        }
    
    return result_dict

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

def get_df_body():
    df_map = {}
    pages_freq_map = {}
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_body")
    rows = c.fetchall()

    word_id_mappings = get_word_id_mappings()

    for row in rows:
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

    page_id_mappings = get_page_id_mappings()

    page_sizes = get_page_body_size_map()

    for word, pages_freq in pages_freq_map.items():
        for url, word_data in pages_freq.items():
            tf = word_data[0]
            weight = (0.5 + (0.5 * (tf / page_sizes[url]))) * math.log2(1 + (len(page_id_mappings.keys())/df_map[word]))
            tf_idf_map[word][url] = weight

    return tf_idf_map # tf_idf_map -> {word : {url : weight}}

def get_tf_idf_score_title():
    tf_idf_map = defaultdict(lambda: defaultdict(float))
    # get the df
    df_map, pages_freq_map = get_df_title()

    page_id_mappings = get_page_id_mappings()

    page_sizes = get_page_body_size_map()

    for word, pages_freq in pages_freq_map.items():
        for url, word_data in pages_freq.items():
            tf = word_data[0]
            weight = (0.5 + (0.5 * (tf / page_sizes[url]))) * math.log2(1 + (len(page_id_mappings.keys())/df_map[word]))
            tf_idf_map[word][url] = weight

    return tf_idf_map  # tf_idf_map -> {word : {url : weight}}

def get_phrase_body_tf_idf(query_phrase):
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

    page_sizes = get_page_body_size_map()

    for phrase, pages_freq in phrase_frequency.items():
        for url, phrase_freq in pages_freq.items():
            tf = phrase_freq
            weight = (0.5 + (0.5 * (tf / page_sizes[url]))) * math.log2(1 + (len(page_id_mappings.keys())/len(phrase_frequency[phrase].keys())))
            tf_idf_map[phrase][url] = weight

    return phrase_frequency, tf_idf_map # phrase_frequency -> {phrase : {url : freq }}, tf_idf_map -> {phrase : {url : weight}}
        

def get_phrase_title_tf_idf(query_phrase):
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

def dot_product(vec1, vec2):
    return sum(a * b for a, b in zip(vec1, vec2))

def vector_magnitude(vec):
    return math.sqrt(sum(a ** 2 for a in vec))

def cosine_similarity(vec1, vec2):
    dot_prod = dot_product(vec1, vec2)
    mag1 = vector_magnitude(vec1)
    mag2 = vector_magnitude(vec2)
    if mag1 == 0 or mag2 == 0:
        return 0
    else:
        return dot_prod / (mag1 * mag2)

def calculate_similarity(query_term, query_phrase, body_weights, title_weights):
    query_term = indexer.index(query_term) 
    query_phrase = indexer.phrase_stemmer(query_phrase)

    all_terms = set(body_weights.keys()).union(set(title_weights.keys())).union(set(query_term).union(set(query_phrase)))

    query_vector = {term: 0 for term in all_terms} # query_vector -> {term : isquery(0/1)}
    for term in query_term + query_phrase:
        query_vector[term] = 1
    
    page_id_mappings = get_page_id_mappings()

    # page_vectors -> {page_id : {word : weight}}
    page_vectors = defaultdict(lambda: defaultdict(float))
    for i in page_id_mappings.keys():
        page_vectors[i] = {}
        for term in all_terms:
            page_vectors[i][term] = 0

    phrase_freq_body, phrase_weight_body = get_phrase_body_tf_idf(query_phrase)
    phrase_freq_title, phrase_weight_title = get_phrase_title_tf_idf(query_phrase)

    for term in all_terms:
        for page_id in page_id_mappings.keys():
            if term in query_phrase: 
                if page_id_mappings[page_id] in phrase_weight_body[term].keys():
                    page_vectors[page_id][term] += phrase_weight_body[term][page_id_mappings[page_id]]
                if page_id_mappings[page_id] in phrase_weight_title[term].keys():
                    page_vectors[page_id][term] += phrase_weight_title[term][page_id_mappings[page_id]] * 5
            elif term in body_weights.keys() and page_id_mappings[page_id] in body_weights[term].keys():
                weight_dict = body_weights[term]
                page_vectors[page_id][term] += weight_dict[page_id_mappings[page_id]]
            elif term in title_weights.keys() and page_id_mappings[page_id] in title_weights[term].keys():
                weight_dict = title_weights[term]
                page_vectors[page_id][term] += weight_dict[page_id_mappings[page_id]] * 5

    query_matrix = list(query_vector.values()) # list of bool values for all terms (0/1)

    page_matrix = {} # {page_id : []}
    for page_id, term_info in page_vectors.items():
        weights = []
        for word, weight in term_info.items():
            weights.append(weight)
        page_matrix[page_id] = weights
            
    pages_similarity = {}

    for page_id, page_vector in page_matrix.items():
        score = cosine_similarity(query_matrix, page_vector)
        pages_similarity[page_id] = round(score, 3)

    updated_pages_similarity = {}
    contains_phrases = {}

    for query in query_phrase:
        for page_id, score in pages_similarity.items():
            if phrase_freq_body[query][page_id_mappings[page_id]] != 0 or phrase_freq_title[query][page_id_mappings[page_id]] != 0:
                contains_phrases[page_id] = True
            else:
                contains_phrases[page_id] = False
    
    for page_id in contains_phrases.keys():
        if contains_phrases[page_id] == True:
            updated_pages_similarity[page_id] = pages_similarity[page_id]

    if len(query_phrase) > 0:
        pages_similarity = updated_pages_similarity
        
    return pages_similarity


def retrieval(query_term, query_phrase):
    body_weights = get_tf_idf_score_body()
    title_weights = get_tf_idf_score_title()

    pages_similarity = calculate_similarity(query_term, query_phrase, body_weights, title_weights)
    print("pages_similarity: ", pages_similarity)
    result = {k: v for k, v in sorted(pages_similarity.items(), key=lambda item: item[1], reverse=True)}
        
    if(len(result) == 0):
        return 0
    elif len(result) >= 50:
        result = result[:50]
    
    return result