# COMP4321 Project

## Folder Structure
##### The file structure consists of 5 files within the backend_code directory:
##### 1. `crawler.py` - Used to define the code and structure of the crawler and the necessary functions needed to extract data from the crawled websites.
##### 2. `indexer.py` - Contains necessary code for stopword removal and stemming of the data obtained from the crawler. This file is also used to run the crawler, apply stopword removal and stemming and populate the tables defined within our database.
##### 3. `db.py` - This file is used to define the overall structure of our database which can be explored more in detail within our schema. It is used to define the tables and necessary functions to populate the tables such as forward indexes, inverted indexes, mapping tables and auxillary tables.
##### 4. `engine.py` - This file is used to handle information retrieval operations within our search engine, and incorporates methodolgies such as TF-IDF, Cosine Similarity and also Phrase Matching.
##### 5. `main.py` - This file is used to run the web crawler. It does so by first initializing our database by creating the tables and then calling the `create_index()` function to run the crawler and then populate the tables within the database. The `main.py` file also consists of code to write the output to the `spider.txt` file.
<br>

## Installation Guide
#### 1. Python (version 3.9 or above)


##### Python version 3.9 or above is recommended to run the crawler since it is compatible with the libraries used in this project. It is also recommended to have the latest version of pip installed to install the necessary libraries.
<br>

#### 2. Libraries
##### The project uses several important external libraries:
- BeautifulSoup4
- urllib
- nltk
- requests
- sqlite3
##### From the above libraries mentioned, we only need to manually install `BeautifulSoup4`, `requests` and `nltk`, the rest of the libraries such as `sqlite3` and `streamlit` are built in within Python.
##### To install the libraries manually, we could use the following pip commands:
- To install BeautifulSoup4, execute `pip install beautifulsoup4` (for more information: https://pypi.org/project/beautifulsoup4/)
- To install nltk, we could execute `pip install nltk` or `pip install --user -U nltk` (for more information: https://www.nltk.org/install.html)
- To install requests, execute `pip install requests` (for more information: https://pypi.org/project/requests/)

##### Additional Note for nltk: In order to ensure the nltk packages work properly, it is recommended to first `import nltk` and then execute `nltk.download('punkt')`, more information for downloading the necessary packages can be found at https://www.nltk.org/data.html. We have added this code to the indexer.py so it will download the necessary nltk packages to run the program. If you already have these packages installed, you may uncomment this line.
<br></br>
## <b>Running or Testing the Crawler (Phase 1)</b>
##### Before running or testing the crawler, please ensure to have the necessary libraries mentioned above installed.
##### As mentioned above, we use the `main.py` file to create our tables and run the crawler as well as the necessary indexing functions to preprocess data and create and populate the tables within our database.
##### By default, our program crawls 30 pages but if you would like to test the crawler to scrape more pages you can modify the `num_pages` parameter in the `create_index()` function in the `indexer.py`. We have left a comment to indicate where you can change the parameter within the `indexer.py` file.
##### For more reference, the line to modify `num_pages` in `indexer.py` (it is the second line within the create_index function)
```python
num_pages = 30 # MODIFY THIS PARAMETER TO CHANGE THE num_pages
```
 #### To run the main file on your terminal or command prompt:
##### 1. Run the `main.py` file
```
python backend_code\main.py
```
Note: On UNIX machines, the character "`/`" should be used in place of "`\`". Hence, you may get an error like this if you try to run it in a UNIX machine:

`[Errno 2] No such file or directory: 'backend_code\\stopwords.txt'`

To solve this, please update the path to `stopwords.txt` accordingly.
#### 2. Examine `spider_result.txt` and `web_crawler.db` (with an appropriate .db file visualizer), it should be stored in the COMP4321-Project directory.
<br>
<br>

## <b>Running the Search Engine (Final Phase)</b>
1. Firstly, ensure that the `num_pages` variable is adjusted accordingly by checking the line in `indexer.py` below. By default, it is already adjusted to the following:
```python
num_pages = 300
```
2. Then, navigate to the project directory in your terminal. Depending on what kind of machine is being used, please run the following lines accordingly:
   
<u>Windows</u>
```
​​python backend_code\main.py
streamlit run backend_code\main.py
```
<u>UNIX-Like Systems (MacOS/Linux)</u>

```
​​python backend_code/main.py
streamlit run backend_code/main.py
```
3. Go to the user interface by opening a web browser and navigating to http://localhost:8501

<i>Note: Depending on the execution time, it will take several minutes to crawl through the pages before the search engine is ready. Please ensure that there are no existing database files or spider output files in the directory before running the application.</i>

