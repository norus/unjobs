from bs4 import BeautifulSoup as bs
from datetime import datetime
from rfeed import *
import requests
import logging
import boto3

logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(level=logging.INFO)

s3 = boto3.resource('s3')


def main(event, context):
    duty_stations = {
        'gva': ('Geneva', 'geneva.xml'),
        'vlc': ('Valencia', 'valencia.xml')
    }

    for duty_station in duty_stations:
        url = 'https://unjobs.org/duty_stations/' + duty_station
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3563.0 Safari/537.36'
        }

        logger.info('INFO: getting {}'.format(url))
        resp = requests.get(url, headers=headers)
        soup = bs(resp.content, features='html.parser')
        divs = soup.findAll('div', {'class': 'job'})

        # Only get divs with jobs
        job_divs = []
        for div in divs:
            if div.get('id') != None:
                job_divs.append(div)

        # Preparing for RSS
        job_items = []
        for job in job_divs:
            item = Item(
                title = job.find('a', {'class': 'jtitle'}).text,
                link = job.find('a', {'class': 'jtitle'}).get('href'),
                description = job.find('a', {'class': 'jtitle'}).text,
                author = job.br.next_sibling,
                guid = Guid(job.find('a', {'class': 'jtitle'}).get('href')),
                    pubDate = datetime.strptime(job.find('time').text, '%Y-%m-%dT%H:%M:%SZ')
            )
            job_items.append(item)

        feed = Feed(
            title = 'UN jobs in {}'.format(duty_stations[duty_station][0]),
            link = 'https://anubis.valiyev.com/unjobs/{}'.format(duty_stations[duty_station][1]),
            description = 'UN jobs in {}'.format(duty_stations[duty_station][0]),
            language = "en-US",
            lastBuildDate = datetime.now(),
            items = job_items
        )

        bucket_name = 'unjobs-xml-bucket'
        file_name = 'unjobs/{}.xml'.format(duty_stations[duty_station][0].lower())
        s3.Bucket(bucket_name).put_object(Key=file_name, Body=feed.rss())
