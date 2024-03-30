import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import sqlite3
import json

class Crawler:
    def __init__(self, base_url, num_pages):
        self.base_url = base_url
        self.num_pages = num_pages
        self.db_conn = sqlite3.connect('web_crawler.db')
        self.c = self.db_conn.cursor()

    def page_id_exists(self, url):
        self.c.execute("SELECT page_id FROM page_mapping WHERE url = ?", (url,))
        page_id = self.c.fetchone()
        if page_id:
            return page_id
        else:
            return False

    def last_modified_date_checker(self, url, last_modified_date):
        page_id = self.page_id_exists(url)
        self.c.execute("SELECT last_modified FROM page_info WHERE page_id = ?", (page_id,))
        prev_last_modified_date = self.c.fetchone()
        prev_last_modified_date = datetime.strptime(prev_last_modified_date, '%a, %d %b %Y %H:%M:%S %Z')
        if last_modified_date > prev_last_modified_date:
            return True
        else:
            return False

    def get_last_modified_date(self, url):
        try:
            response = requests.head(url)
            last_modified_head = response.headers.get("last-modified", 0)
            if last_modified_head:
                last_modified_date = datetime.strptime(last_modified_head, '%a, %d %b %Y %H:%M:%S %Z')
            else:
                last_modified_date = ""
            return last_modified_date
        except requests.exceptions.RequestException:
            return None    

    def get_content(self, soup_obj):
        if soup_obj.title:
            title = soup_obj.title.string
        else:
            title = ""

        body = soup_obj.find('body')
        # todo : remove special characters 

        if body:
            body_text = ' '.join(body.stripped_strings)
        else:
            body_text = ' '.join(soup_obj.stripped_strings)

        # preprocessing on strings
        body_text = body_text.replace('\n', ' ')
        body_text = body_text.replace('\t', ' ')
        body_text = body_text.lower()
        # remove all non ascii chars
        body_text = body_text.encode('ascii', 'replace').decode('ascii')
        body_text = body_text.replace('?', ' ')
        # remove special characters
        body_text = re.sub(r'[^a-zA-Z0-9\s]+',' ', body_text)
        return title, body_text

    def bfs_extract(self):
        # extract the number of pages using BFS
        visited_urls = set()
        unvisited_urls = [self.base_url]

        while unvisited_urls and len(visited_urls) < self.num_pages:
            curr_url = unvisited_urls.pop(0)

            if curr_url in visited_urls:
                continue

            new_links = self.crawl(curr_url, False)
            visited_urls.add(curr_url)

            unvisited_urls.extend(new_links)
        
        return visited_urls
    
    def crawl(self, url):
        try:
            # TODO: DEFINE RULES FOR FETCHING
            page_id = self.page_id_exists(url)
            if page_id != None:
                return None
            response = requests.get(url) # make get request to link to get the data.
            response.raise_for_status()
            
            beautiful_soup = BeautifulSoup(response.content, "html.parser")
            page_counter = 0
            last_modified_date = self.get_last_modified_date(url)
            date_result = self.last_modified_date_checker(url, last_modified_date)
            if date_result == False:
                return None

            links = []
            for link in beautiful_soup.find_all("a", href=True):
                child_link = urljoin(url, link["href"])
                links.append(child_link)
                page_counter += 1

            return links
        
        except requests.exceptions.RequestException:
            return None