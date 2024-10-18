import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

def get_movie_details(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    movie = {}
    movie['name'] = soup.find('h2', class_='m-b-sm').text.strip() if soup.find('h2', class_='m-b-sm') else 'N/A'
    movie['categories'] = [category.text.strip() for category in soup.find_all('button', class_='category')]
    movie['score'] = soup.find('p', class_='score').text.strip() if soup.find('p', class_='score') else 'N/A'
    
    info = soup.find('div', class_='info')
    if info:
        movie['release_date'] = get_info_item(info, '上映日期')
        movie['duration'] = get_info_item(info, '片长')
    else:
        movie['release_date'] = 'N/A'
        movie['duration'] = 'N/A'
    
    movie['description'] = soup.find('p', class_='drama').text.strip() if soup.find('p', class_='drama') else 'N/A'
    movie['url'] = url
    movie['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    return movie

def get_info_item(info, label):
    item = info.find('span', string=lambda text: text and label in text)
    if item and item.next_sibling:
        return item.next_sibling.strip()
    return 'N/A'

def scrape_movies(base_url, num_pages=10):
    movies = []
    
    for page in range(1, num_pages + 1):
        url = f"{base_url}/page/{page}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        movie_links = soup.find_all('a', class_='name')
        
        for link in movie_links:
            movie_url = base_url + link['href']
            try:
                movie = get_movie_details(movie_url)
                movies.append(movie)
                print(f"Scraped: {movie['name']}")
            except Exception as e:
                print(f"Error scraping {movie_url}: {str(e)}")
    
    return movies

def save_to_csv(data, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['name', 'categories', 'score', 'release_date', 'duration', 'description', 'url', 'timestamp']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for movie in data:
            movie['categories'] = ', '.join(movie['categories'])  # Convert list to string for CSV
            writer.writerow(movie)

if __name__ == "__main__":
    base_url = "https://ssr1.scrape.center"
    movies = scrape_movies(base_url)
    save_to_csv(movies, 'ssr1_scrape_center_movies.csv')
    print(f"Scraped {len(movies)} movies and saved to ssr1_scrape_center_movies.csv")