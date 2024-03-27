import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

class Crawler:
    def __init__(self, base_url, num_pages):
        self.base_url = base_url
        self.num_pages = num_pages

    def url_checker(self, url):
        print("TODO")

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

    def get_content(self, soup_obj):# TODO: get Page title, page content
        # get page title from the soup object
        if soup_obj.title:
            title = soup_obj.title.string
        else:
            title = ""

        body = soup_obj.find('body')
        body_text = body.get_text(strip=True)

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
    
    def crawl(self, url, crawl_complete):
        try:
            # TODO: DEFINE RULES FOR FETCHING
            response = requests.get(url) # make get request to link to get the data.
            response.raise_for_status()
            
            beautiful_soup = BeautifulSoup(response.content, "html.parser")
            page_counter = 0
            last_modified_date = self.get_last_modified_date(url)
            title, body_text = self.get_content(beautiful_soup)

            links = []
            for link in beautiful_soup.find_all("a", href=True):
                child_link = urljoin(url, link["href"])
                links.append(child_link)
                page_counter += 1

            if crawl_complete:
                print(f"URL: {url}")
                print(f"Title: {title}")
                print(f"Last Modified: {last_modified_date}")
                print(f"Body: {body_text}")
                print(f"Child links: {links} \n")

            return links
        
        except requests.exceptions.RequestException:
            return None

if __name__ == "__main__":
    extracted_links = []
    base_url = "https://www.cse.ust.hk/~kwtleung/COMP4321/ust_cse.htm"
    num_pages = 30
    crawler = Crawler(base_url, num_pages)
    bfs_extract = crawler.bfs_extract()

    # print(f"Extracted links: ")
    # i = 0
    bfs_extract = sorted(bfs_extract)
    # for x in bfs_extract:
    #     i += 1
    #     print(f"{i}. {x}")

    page_no = 1
    for link in bfs_extract:
        print(f"Page {page_no}")
        page_no += 1
        crawl = crawler.crawl(link, True)
