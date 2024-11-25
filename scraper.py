from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re
from urllib.parse import urljoin
from time import sleep

class Scraper:

    def __init__(self, companies, links):
        self.links = links
        self.companies = companies
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://google.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Connection': 'keep-alive',
            'DNT': '1',  # Do Not Track Request Header
            'Upgrade-Insecure-Requests': '1'
        }
        self.data = []
        self.session = requests.Session()  

       
        self.keywords = {
            "F&B": {"food processing", "beverage", "snacks", "dairy", "bakery", "juices", 
                    "packaged foods", "ready-to-eat", "functional foods", "culinary"},
            "Manufacturing": {"manufacturer", "production", "factory", "assembly", "fabrication", 
                              "mass production", "packaging", "bottling"},
            "Brand": {"brand", "product launch", "branding", "consumer trends", "trademark", "logo", 
                      "marketing", "advertising"},
            "Distribution": {"distributor", "wholesaler", "supplier", "logistics", "supply chain", 
                             "sourcing", "warehousing", "fulfillment", "shipping", "delivery","pharma","pharmaceuticals","drugs"},
            "Probiotics": {"probiotics", "gut health", "digestive health", "fermented", "live cultures", 
                           "healthy bacteria", "prebiotics", "synbiotics", "microbiome", "supplements"}
        }

        # Initialize a list of all keywords
        self.all_keywords = set().union(*self.keywords.values())
    
    def scrap_links(self):
        for link in self.links:
            try:
                self.scrap_link(link)
            except Exception as e:
                print(f"Error scraping link {link}: {e}")

        # Convert collected data to a DataFrame
        df = pd.DataFrame(self.data)
        
        excel_filename = "company_fnb_classification.xlsx"
        df.to_excel(excel_filename, index=False)
        print(f"Data saved to {excel_filename}")
    
    def scrap_link(self, link):
        print(f"Scraping: {link}")
        link = f"https://{link.strip()}?"
        response = self.fetch_with_retries(link)
        
        if not response:
            print(f"Failed to retrieve data from {link}")
            return
        
        soup = BeautifulSoup(response.content, 'html.parser')
        about_pages = self.find_about_page(soup)
        json_data = self.initialize_record()
        
        # Scrape each "about" page
        for url in about_pages:
            json_data = self.scrap_about_page(link, url, json_data)
        
        self.data.append(json_data)

    def fetch_with_retries(self, url, retries=3, delay=2):
        """Retries fetching the URL in case of network issues."""
        for attempt in range(retries):
            try:
                response = self.session.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()  # Raise HTTPError for bad responses
                return response
            except requests.exceptions.RequestException as e:
                print(f"Connection error: {e}. Retrying ({attempt+1}/{retries})...")
                sleep(delay)
        return None
    
    def initialize_record(self):
       
        return {keyword: False for keyword in self.all_keywords}
    
    def scrap_about_page(self, base_link, url, json_data):
        
        if not url.startswith("https://"):
            url = urljoin(base_link, url)
        
        response = self.fetch_with_retries(url)
        if not response:
            return json_data
        
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.get_text()
        
        
        for category, keywords in self.keywords.items():
            for word in keywords:
                if re.search(rf'\b{word}\b', text, re.IGNORECASE):
                    json_data[word] = True
        
       
        for category, keywords in self.keywords.items():
            json_data[category] = any(json_data[feature] for feature in keywords)
        
       
        json_data['Relevant'] = json_data['Probiotics'] or json_data['Distribution']
        json_data['Company Name'] = (base_link[11:])[:-4]
        return json_data
    

    def find_about_page(self, soup):
       
        links = []
        search = soup.find_all('a', href=True)
        for link in search:
            if re.search(r'\babout\b', link.text, re.IGNORECASE) or re.search(r'\babout\b', link['href'], re.IGNORECASE):
                links.append(link['href'])
        return links

if __name__ == "__main__":
    # Load input data
    df = pd.read_csv("Rename.csv", delimiter=",")
    df.columns = ['col1', 'col2', 'col3', 'col4']
    
    # Start the scraping process
    scraper = Scraper(df['col3'][4:].values, df['col4'][4:].values)
    scraper.scrap_links()
