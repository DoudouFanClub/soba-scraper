"""
Things to account for:
- Format dupe newlines / whitespace / html entities / tabs / unicode / font styles
- Check if "header" exists before accessing, else skip file
  - Save the missed url in a separate text file for subsequent reference
  - Save duplicate urls in separate text file
- Select "contents" / "toc" / "textblock" and "title" and decide how to save text
- Url consider reading until ".html" since suffix are hyperlinks within current page
- See if theres a way to start from base page for all links, then access a particular
  section for the subsequent hyperlinks
"""
import os
import re
import json
import requests
import unicodedata
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

class SobaScraper:
    def __init__(self, config_path):
        self.config_path = config_path

    def scrape_all(self):
        # Read config.json - Add safety after
        cfgs = ''
        with open(self.config_path, 'r') as file:
            cfgs = json.load(file)

        for cfg in cfgs:
            # Create Missing Dir
            out_dir = os.path.dirname(__file__) + cfg['outputFolderPath']
            os.makedirs(out_dir, exist_ok=True)
            self.scrape_webpage(out_dir, cfg['baseUrl'], cfg['headerRetrievalSection'], cfg['textRetrievalSection'], cfg['linkRetrievalSection'])
    
    def extract_from_url(self, url, header_section, text_section, link_section):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        header = self.extract_header(soup, header_section)
        text = self.extract_text(soup, text_section)
        link = self.extract_link(soup, link_section)

        return header, text, link
    
    def extract_header(self, soup, section=None):
        # header = ""
        # if section:
        #     # Find the div with the class that matches the section name
        #     section_soup = soup.find('div', class_=section)
        #     for header_tag in section_soup.find_all(class_='ingroups'): # change the reading of json later
        #         header_tag.decompose()
        #     header = section_soup.get_text(' - ', strip=True)
        
        return ''

    def extract_text(self, soup, section=None):
        text = ''
        section_soup = soup.find('div', class_=section)
        if section_soup:
            text = section_soup.get_text()
        return text

    def extract_link(self, soup, section=None):
        links = []
        section_soup = soup.find('div', class_=section)
        if section_soup:
            links = [a.get('href').split('#')[0] for a in section_soup.find_all('a', href=True)]
        return links
    
    def write_text_to_header(self, out_dir, header, text):
        with open(out_dir + '\\' + header + '.txt', 'w') as file:
            """
            Perform Formatting...
            """
            print('writing to: ', out_dir + '\\' + header + '.txt')
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

    def scrape_webpage(self, out_dir, url, header_retrieval_section, text_retrieval_section, link_retrieval_section):
        visited_links = set()
        links_to_visit = { url }

        # Iterate until there are no more links
        while links_to_visit:
            link = links_to_visit.pop()

            # Process only if unique link
            if link not in visited_links:
                visited_links.add(link)
                print(f"Visiting {link}...")

                # Extract text
                header, text, new_links = self.extract_from_url(link, header_retrieval_section,
                                                                text_retrieval_section, link_retrieval_section)
                
                # print('header: ', header)
                # print('text: ', text)
                # print('new_links: ', new_links)

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
                        links_to_visit.add(new_link)

        # Write a summary file of scraped urls
        with open(out_dir + '\\Summary.txt', 'w') as file:
            file.write('Urls Scraped Successfully:\n')
            file.write('\n'.join(visited_links))