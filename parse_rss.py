#!/usr/bin/env python3

# from bs4 import BeautifulSoup
# import requests
import logging
import feedparser
import pprint

# Logging Options
LOGGING_LEVEL = 'INFO'
# RSS feed URL
RSS_URL = 'http://infotechsourcing.crelate.com/portal/rss'


def cleanup_published(data=''):
    # Example: 'Mon, 11 Dec 2017 21:27:30 Z'
    # For some reason the published data has a 'Z' on the end of it.
    return data[0:-2]


def cleanup_tags(data=''):
    """
    Example:
        [
            {'term': 'Github', 'scheme': None, 'label': None},
            {'term': 'Technical Writer', 'scheme': None, 'label': None},
            {'term': 'API documentation', 'scheme': None, 'label': None}
        ]"
    We just want a list of "terms".
    """
    terms = []
    # Some RSS entries don't have tags and will only have value "None".
    if data:
        for item in data:
            terms.append(item.get('term'))

    return terms



def main():
    # Get RSS feed and convert into dict
    rss_dict = feedparser.parse(RSS_URL)

    # The desired data is a list of dicts under the rss_dict['entries']
    # Build a list of SQL statements in preparation for rebuild of SQL database.
    sql_statements = []
    all_tags = []
    for entry in rss_dict['entries']:
        jobnumber = entry.get('jobnumber')
        link = entry.get('link')
        location = entry.get('location')
        published = cleanup_published(data=entry.get('published'))
        summary = entry.get('summary')
        title = entry.get('title')
        tags = cleanup_tags(data=entry.get('tags'))

        # Build a list of unique tag values.
        for tag in tags:
            if tag not in all_tags:
                all_tags.append(tag)

        sql_statements.append(
            "insert into DATABASE values ({}, {}, {}, {}, {}, {}, {});".format(jobnumber,
                                                                               link,
                                                                               location,
                                                                               published,
                                                                               summary,
                                                                               title,
                                                                               tags))

    # Add tags to DATABASE
    print(all_tags)
    # Add jobs to DATABASE TODO:(Need to reference tag PK instead of tag word.)
    print(sql_statements)


if __name__ == '__main__':
    logging.basicConfig(level=LOGGING_LEVEL)
    main()
