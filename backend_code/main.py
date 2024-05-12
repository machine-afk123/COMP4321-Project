import db
import indexer
import engine
import streamlit as st

def process_input(input_string):
    individual_words = []
    word_groups = []

    inside_group = False
    current_group = ""
    current_word = ""

    for character in input_string:
        if character == '"':
            if inside_group:
                word_groups.append(current_group)
                current_group = ""
                inside_group = False
            else:
                inside_group = True
                if current_word:
                    individual_words.append(current_word)
                    current_word = ""
        elif character == " ":
            if inside_group:
                current_group += character
            else:
                if current_word:
                    individual_words.append(current_word)
                    current_word = ""
        else:
            if inside_group:
                current_group += character
            else:
                current_word += character

    if current_word:
        individual_words.append(current_word)
    if current_group:
        word_groups.append(current_group)

    return individual_words, word_groups

def view_results(output):
    result_dict = engine.get_page_info(output.keys())

    for key, value in result_dict.items():
        st.subheader(f"{value["title"]}")
        st.markdown(f"Score: {output[key]}")
        st.markdown(f"URL: {str(value["url"])}")
        st.markdown(f"Last Modified Date: {str(value["last_modified"])}")
        st.markdown(f"Page Size: {value["page_size"]}")
        keyword_freq_str = ""
        for word, freq in value["keywords"].items():
            keyword_freq_str += str(word) + " " + str(freq) + "; "
        keyword_freq_str = keyword_freq_str[:len(keyword_freq_str)-2]
        st.markdown(keyword_freq_str)
        st.markdown(f"Parent Links:")
        for parent in value["parent_links"]:
            st.markdown(str(parent))
        st.markdown(f"Child Links:")
        for child in value["child_links"]:
            st.markdown(str(child))
        st.divider()

def app():
    left_co, cent_co, last_co = st.columns(3)
    with cent_co:
        st.image("red_bird.png")
        st.header("Search Engine", divider="red")
    input_query = st.text_input("Enter your query here")

    if st.button("SEARCH"):
        query_term, query_phrase = process_input(input_query)
        output = engine.retrieval(query_term, query_phrase)
        view_results(output)
    else:
        output = {}
    
def main():
    conn, c = db.init_connection()
    db.create_tables(conn, c)
    indexer.create_index(conn, c) 
    app()
    db.close_connection(conn)

if __name__ == "__main__":
    main()