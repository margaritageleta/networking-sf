from quart import Quart
from quart import render_template
import pandas as pd
import datetime as dt
import calendar
import numpy as np
import requests
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession
import time

app = Quart(__name__)

def extract_date(row):
    month2id = dict(zip(list(calendar.month_name)[1:], range(1,12+1)))
    month = month2id[row[2].split()[0]]
    year = row[0]
    day = int(row[2].split()[-1].split('-')[0])
    time_object = row[3].split('(')[0].strip()
    time_object = time_object.replace("–", "-")
    pm = time_object[-2:]
    time_object = time_object.split('-')[0].strip().replace(" ", "")
   
    # That's when 4-5pm instead of 4:00PM - 5:00PM
    if not (time_object[-2:].upper() == 'PM' or time_object[-2:].upper() == 'AM'):
        time_object += pm
    # That's when 4PM instead of 4:00PM
    if len(time_object) <= 4:
        time_object = time_object[:-2] + ':00' + time_object[-2:]
    
    time_object = f'{year}-{month}-{day} {time_object}'
    date = dt.datetime.strptime(time_object,'%Y-%m-%d %I:%M%p') 
    return date

def download_from_cerebral_valley():
    df = pd.read_csv('downloaded_content/cerebral_valley_sheet', header=5)
    replaced_ny = False
    for i in range(df.shape[0]):
        if not replaced_ny:
            if len(str(df.loc[i,'Month'])) == 4:
                replaced_ny = True
                df.loc[:i,'Month'] = int(df.loc[i,'Month'])
                df.loc[i:,'Month'] = int(df.loc[i,'Month']) - 1
        else: break
    df = df[df['Date'].notna()]
    df['Start Time'] = df.apply(lambda x: extract_date(x), axis=1)
    # filter out old ones
    # df = df[~(df['Start Time'] < dt.datetime.now())]
    df = df.drop(['Month', 'Date', 'Time '], axis=1)
    df['Source'] = 'Cerebral Valley (gsheet)'
    return df

def download_from_luma_listing(listing_name, listing_uri):
    page = requests.get(listing_uri)
    soup = BeautifulSoup(page.content, "html.parser")
    timeline_sections = soup.find_all("div", class_="timeline-section")
    df = []

    for day in timeline_sections:
        date_ = day.select('.date')
        #date = soup.find_all("div", class_=".date")

        event_links = day.select('.event-link')
        event_contents = day.select('.event-content')
        events = zip(event_links, event_contents)

        for link, info in events:
            date = date_[0].text
            if date == 'Today':
                date = dt.datetime.now()
                time_object = f'{date.year}-{date.month}-{date.day}'
            elif date == 'Yesterday':
                date = dt.datetime.now() - dt.timedelta(days=1)
                time_object = f'{date.year}-{date.month}-{date.day}'
            elif date == 'Tomorrow':
                date = dt.datetime.now() + dt.timedelta(days=1)
                time_object = f'{date.year}-{date.month}-{date.day}'
            else:
                date = date.split(',')
                month2id = dict(zip(list(calendar.month_abbr)[1:], range(1,12+1)))
                if len(date) > 1:
                    year = int(date[-1])
                else:
                    year = dt.datetime.now().year
                date = date[0]
                date = date.split(' ') 
                month = int(month2id[date[0]])
                day = int(date[-1])
                time_object = f'{year}-{month}-{day}'

            time_object += ' 12:00PM'  
            location = 'San Francisco, CA'

            date = dt.datetime.strptime(time_object,'%Y-%m-%d %I:%M%p') 
            if link['href'][0] =='/':
                link['href'] = 'https://lu.ma' + link['href']

            record = {
                'Event': info.select('h3')[0].text,
                'Location': location,
                'Link': link['href'],
                'Start Time': date,
                'Source': listing_name
            }
            df.append(record)

    df = pd.DataFrame(df)
    # df = df[~(df['Start Time'] < dt.datetime.now())]
    return df

def partiful_scraper(partiful_uri):
    
    page = requests.get(partiful_uri)
    soup = BeautifulSoup(page.content, "html.parser")

    time = soup.select('.dtstart > div')
    date = time[0].text.split(',')[-1].strip().split(' ')
    month2id = dict(zip(list(calendar.month_abbr)[1:], range(1,12+1)))
    month = int(month2id[date[0].strip()])
    day = int(date[1])
    if dt.datetime.now().month == 12 and month < 4:
        year = dt.datetime.now().year + 1
    else: year = dt.datetime.now().year
    
    time_object = time[1].text[:-5]
    time_object = time_object.replace("–", "-")
    pm = time_object[-2:]
    time_object = time_object.split('-')[0].strip().replace(" ", "")
   
    # That's when 4-5pm instead of 4:00PM - 5:00PM
    if not (time_object[-2:].upper() == 'PM' or time_object[-2:].upper() == 'AM'):
        time_object += pm
    # That's when 4PM instead of 4:00PM
    if len(time_object) <= 4:
        time_object = time_object[:-2] + ':00' + time_object[-2:]
    
    time_object = f'{year}-{month}-{day} {time_object}'
    date = dt.datetime.strptime(time_object,'%Y-%m-%d %I:%M%p') 
    
    return date
    

async def metaphor_scraper(listing_name, listing_uri):

    async def get_website(url: str):
        asession = AsyncHTMLSession() 
        r = await asession.get(url)
        await r.html.arender(sleep = 3) # sleeping is optional but do it just in case
        html = r.html.raw_html # this can be returned as your result
        await asession.close() # this part is important otherwise the Unwanted Kill.Chrome Error can Occur 
        return html
    
    html = await get_website(listing_uri)
    soup = BeautifulSoup(html, "html.parser")

    df = []
    for a, span in zip(soup.select("[class^=SearchResultstyles__UrlLink]"), soup.select("[class^=SearchResultstyles__TitleLink]")):
        
        try:
            date = partiful_scraper(a['href'])

            record = {
                'Event': span.text,
                'Location': None,
                'Link': a['href'],
                'Start Time': date,
                'Source': listing_name
            }
            df.append(record)
        except Exception as e:
            print(f'Private event that needs password: {a["href"]}')
    df = pd.DataFrame(df)
    # df = df[~(df['Start Time'] < dt.datetime.now())]
    return df


@app.route("/")
async def load_events():
    
    luma_listings = [
        ('Generative AI SF (luma listing)','https://lu.ma/genai-sf'),
        ('SF (luma listing)','https://lu.ma/sf'),
        ('The GenAI Collective (luma listing)', 'https://lu.ma/gaico'),
        ('Thursday Nights in AI (luma listing)', 'https://lu.ma/thursday-ai'),
        ('Tribe SF (luma listing)', 'https://lu.ma/tribe'),
        ('The AI Salon (luma listing)', 'https://lu.ma/ai-salon'),
        ('Startup Social SF (luma listing)', 'https://lu.ma/startupsocial'),
        ('Spice King (luma listing)', 'https://lu.ma/spicekingofzanzibar'),
        ('MindsDB (luma listing)', 'https://lu.ma/mindsdb')
    ]

    partiful_listings = [
        ('Metaphor RSVP AI (partiful listing)', 'https://search.metaphor.systems/search?q=RSVP%20AI&filters=%7B%22timeFilterOption%22%3A%22past_month%22%2C%22domainFilterType%22%3A%22include%22%2C%22includeDomains%22%3A%5B%22partiful.com%22%5D%7D'),

    ]

    print(f'Downloading from listing Cerebral Valley')
    data = download_from_cerebral_valley()

    for (listing_name, listing_uri) in luma_listings:
        print(f'Downloading from listing {listing_name}: {listing_uri}')
        df = download_from_luma_listing(listing_name, listing_uri)
        data = pd.concat([data, df])
        del df

    for (listing_name, listing_uri) in partiful_listings:
        print(f'Downloading from listing {listing_name}: {listing_uri}')
        df = metaphor_scraper(listing_name, listing_uri) 
        data = pd.concat([data, await df])
        del df

    # Drop duplicates
    data = data.groupby('Event',sort=False).apply(lambda x: x if len(x)==1 else x.loc[x.Source.eq('Cerebral Valley (gsheet)')]).reset_index(drop=True)
    data = data.groupby('Link',sort=False).apply(lambda x: x if len(x)==1 else x.loc[x.Source.eq('Cerebral Valley (gsheet)')]).reset_index(drop=True)
    data = data.sort_values(by='Start Time', ascending=False)

    data = data[~(data['Start Time'] < dt.datetime.now() -  dt.timedelta(days=15))]

    return await render_template('main.html', fields=data.columns.values, data=list(data.values.tolist()), 
                            zip=zip, link_column="Link")


app.run(host='localhost', port=5000)