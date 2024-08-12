import os
import re
import json
import time
import requests
import threading
import unicodedata
from queue import Queue
from bs4 import BeautifulSoup
from threadsafe_set import Threadsafe_Set
from threadsafe_bool import Threadsafe_Boolean
from urllib.parse import urljoin, urlparse

class SobaScraper:
    def __init__(self, config_path, num_workers=5):
        assert num_workers > 0, "Must have at least 1 worker thread to scrape data..."
            
        self.config_path = config_path
        self.num_workers = num_workers

        self.workers = []
        self.config_details = []
        self.url_queue = Queue()
        self.engaged = Threadsafe_Boolean()
        self.visited_links = Threadsafe_Set()

        
    def restart_workers(self, first_url, out_dir, text_retrieval_section, link_retrieval_section):
        self.workers = []
        self.url_queue = Queue()
        self.url_queue.put(first_url)
        self.visited_links.clear()

        for _ in range(self.num_workers):
            worker = threading.Thread(
                target=self.worker_thread,
                args=(out_dir, text_retrieval_section, link_retrieval_section, 3, 5)
            )
            worker.start()
            self.workers.append(worker)

        # Await all threads
        for worker in self.workers:
            worker.join()

    def scrape_all(self):
        # Read config.json
        with open(self.config_path, 'r') as file:
            self.config_details = json.load(file)

        for cfg in self.config_details:
            # Create Missing Dir
            out_dir = os.path.dirname(__file__) + cfg['outputFolderPath']
            os.makedirs(out_dir, exist_ok=True)

            # Mark the System to be "Engaged"
            self.engaged.disable()
            self.scrape_webpage(
                out_dir,
                cfg['baseUrl'],
                cfg['textRetrievalSection'],
                cfg['linkRetrievalSection']
            )
            self.engaged.wait_for_completion()

    def ping_url_for_response(self, url, retries, duration):
        response = None

        while retries > 0:
            err = None
            try:
                response = requests.get(url)
                response.raise_for_status()
                break
            except requests.exceptions.ConnectionError as e:
                err = e
                retries -= 1
                if retries == 0:
                    return None, True
            except requests.exceptions.RequestException as e:
                err = e
                retries -= 1

            if retries >= 0:
                print(f"Error: {err}. Retrying in {duration} seconds...")
                time.sleep(duration)
            else:
                print(f"Error: {err}. Skipping this url...")

        # Returning True means we wish to terminate
        # due to Connection Error
        return response, False

    def extract_from_url(self, url, text_section, link_section, retries, duration):
        response, terminate = self.ping_url_for_response(url, retries, duration)
        
        # Terminate when we encounter network error for
        # retries * duration period of time
        if terminate:
            return None, None, terminate
        
        # Could fail to ping the website anyways
        # Just return empty
        if response == None:
            return '', [], False

        soup = BeautifulSoup(response.content, 'html.parser')
        text = self.extract_text(soup, text_section)
        links = self.extract_link(soup, link_section)
        return text, links, False

    def extract_text(self, soup, section=None):
        if section != '':
            section_soup = soup.find('div', class_=section)
            if section_soup:
                return section_soup.get_text()
        return ''

    def extract_link(self, soup, section=None):
        if section != '':
            section_soup = soup.find('div', class_=section)
            if section_soup:
                return [a.get('href').split('#')[0] for a in section_soup.find_all('a', href=True)]
        return []
    
    def write_text_to_header(self, out_dir, header, text):
        file_path = out_dir + f'{header}.txt'
        with open(file_path, 'w') as file:
            print(f'Writing to: {file_path}')
            # Remove Unicode
            text = unicodedata.normalize('NFKD', text).encode('ascii', errors='ignore').decode('ascii')
            # Replace Cascading ()'s on different lines due to <td>
            text = re.sub(r'\s\(\s', '( ', text)
            text = re.sub(r'\s([a-zA-Z_]\w*)\)', r'\1 )', text)
            # Remove Whitespace
            text = re.sub(r'\n+', '\n', text)
            # Remove ' More...' from Hyperlinks
            text = text.replace(' More...', '')
            # Remove Tabs
            text = re.sub(r'\t+', '', text)

            # Write text to the file
            file.write(str(text))
            file.close()

    def scrape_webpage(self, out_dir, url, text_retrieval_section, link_retrieval_section):
        # Set the System Status to engaged
        self.engaged.disable()
        print('=========== STARTED SCRAPING ===========')
        # Start the worker threads here
        self.restart_workers(url, out_dir, text_retrieval_section, link_retrieval_section)
        print('=========== STOPPED SCRAPING ===========')

        # Wait for System Status to be disengaged by Worker Thread
        self.engaged.wait_for_completion()

        # Write a summary file of scraped urls
        with open(out_dir + 'Summary.txt', 'w') as file:
            file.write('Urls Scraped Successfully:\n')
            file.write('\n'.join(self.visited_links.get_copy()))

    """
        Create a worker thread that retries 3 times, with 5s intervals
        if there are no more urls to visit in the url_queue
        Acts as a precaution in case the first URL takes too long to
        parse and no other URLs are available to be visited
    """
    def worker_thread(self, out_dir, text_retrieval_section, link_retrieval_section, retries=3, duration=5):
        while self.url_queue.qsize() < 0 or retries > 0: # check here and sleep prolly do if true and then check queue

            if self.url_queue.qsize() == 0:
                print(f"Queue is empty on thread {threading.current_thread().ident}... Sleeping...")
                time.sleep(duration)
                retries -= 1
                continue

            retries = 3
            link = self.url_queue.get()

            # We do not want to open any pdf files by accident and scrape them
            # Ignore visited links also
            if '.pdf' in link or self.visited_links.contains(link):
                continue

            # Append visited urls
            self.visited_links.add(link)
            print(f"Visiting {link} on thread {threading.current_thread().ident}...")

            # Extract text & links
            text, new_links, network_terminate = self.extract_from_url(
                link,
                text_retrieval_section,
                link_retrieval_section,
                retries,
                duration
            )

            # Terminate if encountering Network Error
            if network_terminate:
                self.engaged.enable()
                return
            # If we have no text, dont bother writing the file
            if text == '':
                continue

            # Create missing directory if needed for outdir
            url_header = urlparse(link).path.split('/')[-1]
            url_header = url_header.replace('.html', '')
            if not os.path.isfile(out_dir + '\\' + url_header + '.txt'):
                self.write_text_to_header(out_dir, url_header, text)

            # Add new found urls within '<div class=cfg['linkRetrievalSection']>'
            for new_link in new_links:
                if new_link.startswith('#'):
                    continue
                new_link = urljoin(link, new_link)
                if urlparse(new_link).netloc == urlparse(link).netloc:
                    self.url_queue.put(new_link)
        
        # Upon completion
        self.engaged.enable()