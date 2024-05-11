import db
import indexer
import sqlite3
import json
import engine

def handle_query(query):
    single_terms = []
    phrases = []

    in_phrase = False
    current_phrase = ""
    current_term = ""

    for i in query:
        if i == '"':
            if in_phrase:
                phrases.append(current_phrase)
                current_phrase = ""
                in_phrase = False
            else:
                in_phrase = True
                if current_term:
                    single_terms.append(current_term)
                    current_term = ""
        elif i == " ":
            if in_phrase:
                current_phrase += i
            else:
                if current_term:
                    single_terms.append(current_term)
                    current_term = ""
        else:
            if in_phrase:
                current_phrase += i
            else:
                current_term += i

    if current_term:
        single_terms.append(current_term)
    if current_phrase:
        phrases.append(current_phrase)


    return single_terms, phrases
    
def main():
    conn, c = db.init_connection()
    db.create_tables(conn, c)
    indexer.create_index(conn, c) 
    # CALL SEARCHER HERE
    # query = st.textinput()
    query_term, query_phrase = handle_query('"Movie Index Page"')
    print(query_term)
    print(query_phrase)
    result = engine.retrieval(query_term, query_phrase)
    # generate component cards for each page result.

    print(result)

    # result_front_end = []

    # for doc_id, score in result:
    #     current_result = []
    #     current_result.append("Score: " + str(score) + "    " + crawled_result[doc_id]["title"])
    #     current_result.append("  URL: " + crawled_result[doc_id]["url"])

    #     current_result.append("  " + "Last modified time: " + crawled_result[doc_id]["last_modified"] + ", Page size: " + str(crawled_result[doc_id]["page_size"]) + '\n')
        
    #     top_n_words = crawler.top_n_keywords(crawled_result[doc_id]["keywords"], 5)
    #     top_n_words_str = "  Top 5 keywords: "
    #     for word in top_n_words:
    #         top_n_words_str += (word + " " + str(crawled_result[doc_id]["keywords"][word]["frequency"]) + "; ")
    #     current_result.append(top_n_words_str + '\n')

    #     current_result.append("  Parent links: ")
    #     count = 0
    #     for parent in crawled_result[doc_id]["parents"]:
    #         current_result.append("    " + parent)
    #         count += 1
    #         if count == 10:
    #             current_result.append("    ......\n")
    #             break

    #     current_result.append("  Children links: ")
    #     count = 0
    #     for child in crawled_result[doc_id]["children"]:
    #         current_result.append("    " + child)
    #         count += 1
    #         if count == 10:
    #             current_result.append("    ......\n")
    #             break
    #     current_result.append('\n')

    #     for string in current_result:
    #         string = replace_chars(string)
    #     result_front_end.append(current_result)

    # print(result_front_end)
    # print("length: ", len(result_front_end))
    # print("========================Search End========================")

    # return result_front_end
    db.close_connection(conn)

if __name__ == "__main__":
    main()