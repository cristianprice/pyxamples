import scrapelib

s = scrapelib.Scraper(requests_per_minute=10)

# Grab Google front page
s.get('http://google.com')

# Will be throttled to 10 HTTP requests per minute

response = s.get('http://example.com')
print(response.content)
