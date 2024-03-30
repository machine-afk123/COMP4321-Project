import db
import indexer
import sqlite3
import json
    
def main():
    # db.create_tables()
    # indexer.create_index()

    db_conn = sqlite3.connect('web_crawler.db')
    c = db_conn.cursor()    
    c.execute("SELECT word_id, pages_freq FROM invertedIndex_body")
    rows = c.fetchall()

    def get_word_freq(url):
        keyword_freq_map = {}
        c.execute("SELECT word_id, pages_freq FROM invertedIndex_body")
        rows_inv_index_body = c.fetchall()
        
        for row in rows_inv_index_body:
            pages_freq = json.loads(row[1])
            for page, info in pages_freq.items():
                if page == url:
                    freq = info[0]
                    c.execute("SELECT word FROM stemmed_mapping WHERE word_id=?", (row[0],))
                    word = c.fetchone()[0]
                    keyword_freq_map[word] = freq
        
        c.execute("SELECT word_id, pages_freq FROM invertedIndex_title")
        rows_inv_index_title = c.fetchall()
        for row in rows_inv_index_title:
            pages_freq = json.loads(row[1])
            for page, info in pages_freq.items():
                if page == url:
                    freq = info[0]
                    c.execute("SELECT word FROM stemmed_mapping WHERE word_id=?", (row[0],))
                    word = c.fetchone()[0]
                    if word not in keyword_freq_map.keys():
                        keyword_freq_map[word] = freq
                    else:
                        keyword_freq_map[word] += freq

        return keyword_freq_map
    
    c.execute("SELECT page_id, page_size, last_modified,title, child_links FROM page_info")
    rows = c.fetchall()
    with open('spider.txt', 'w') as f_obj:
        for row in rows:
            page_id = row[0]
            f_obj.write(f"{row[3]}\n")
            c.execute("SELECT url FROM page_mapping WHERE page_id=?", (page_id,))
            url = c.fetchone()
            f_obj.write(f"{url[0]}\n")
            f_obj.write(f"{row[2]} {row[1]}\n")

            keyword_freq_map = get_word_freq(url[0])
            keyword_counter = 0
            for keyword, freq in keyword_freq_map.items():
                if keyword_counter >= 10:
                    break
                f_obj.write(f"{keyword} {freq}; ")
                keyword_counter += 1
            f_obj.write("\n")

            child_links = row[4].split(',')
            link_counter = 0
            for link in child_links:
                if link_counter >= 10:
                    break
                f_obj.write(f"{link}\n")
                link_counter += 1

            f_obj.write("-------------------------------------------------------------------\n")
    
    db_conn.close()

if __name__ == "__main__":
    main()