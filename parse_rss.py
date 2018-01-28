#!/usr/bin/env python3

import logging
import feedparser
import pymysql
import userdata
import re

# Regex for identifying HTML tags.
TAG_RE = re.compile(r'<[^>]+>')
BLANKLINE_RE = re.compile(r'\s{2,}')

# Number of characters of the summary to keep
SUMMARY_CHARS = 500

# Logging Options
LOGGING_LEVEL = 'INFO'

# RSS feed URL
RSS_URL = 'http://infotechsourcing.crelate.com/portal/rss'

# SQL statements to drop and create tables.
drop_job2tags = "DROP TABLE IF EXISTS job2tags"
drop_jobs = "DROP TABLE IF EXISTS jobs"
create_job2tags = "CREATE TABLE `job2tags` ( `jobnumber` BIGINT UNSIGNED NOT NULL , " \
              "`tag` TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL ) ENGINE = InnoDB;"
create_jobs = "CREATE TABLE `jobs` ( `jobnumber` BIGINT UNSIGNED NOT NULL , " \
              "`link` TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL , " \
              "`published` TEXT CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL , " \
              "`summary` LONGTEXT CHARACTER SET utf8 COLLATE utf8_bin NULL DEFAULT NULL , " \
              "`title` TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL , " \
              "`location` TEXT CHARACTER SET utf8 COLLATE utf8_bin NOT NULL , " \
              "PRIMARY KEY (`jobnumber`)) ENGINE = InnoDB;"

# SQL for inserting into job2tags
insert_job2tags = """INSERT INTO job2tags values ("{}", "{}");"""

# SQL for inserting into jobs
insert_job = """INSERT INTO jobs values ("{}", "{}", "{}", "{}", "{}", "{}");"""

"""
# Other needed SQL statements not yet implemented.
    SELECT DISTINCT location FROM jobs
    SELECT COUNT(*) FROM jobs WHERE location='{}'
    SELECT DISTINCT tag FROM job2tags
    SELECT COUNT(DISTINCT tag) FROM job2tags
    SELECT jobnumber, link, title, location, summary FROM `jobs`
    SELECT jobnumber, link, title, location, summary FROM `jobs` WHERE jobnumber='{}'
    SELECT jobnumber, link, title, location, summary FROM `jobs` WHERE location='{}'
    SELECT jobs.jobnumber, link, title, location, summary FROM `jobs` LEFT JOIN job2tags ON jobs.jobnumber=job2tags.jobnumber WHERE tag='{}'
"""


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
            if item not in terms:
                terms.append(item.get('term'))

    return terms


def remove_tags(text):
    text = TAG_RE.sub('  ', text)
    return BLANKLINE_RE.sub(' ', text)


def cleanup_summary(data=''):
    return remove_tags(data)[0:SUMMARY_CHARS]


def main():
    # Open database connection
    with pymysql.connect("localhost", userdata.DB_USERNAME, userdata.DB_PASSWORD, userdata.DB, charset='utf8') as db:
        # Delete the existing tables to clear out any old information.
        db.execute(drop_job2tags)
        db.execute(drop_jobs)

        # Recreate the tables.
        db.execute(create_job2tags)
        db.execute(create_jobs)

        # Get RSS feed and convert into dict
        rss_dict = feedparser.parse(RSS_URL)

        # The desired data is a list of dicts under the rss_dict['entries']
        for entry in rss_dict['entries']:
            jobnumber = pymysql.escape_string(entry.get('jobnumber'))
            link = pymysql.escape_string(entry.get('link'))
            location = pymysql.escape_string(entry.get('location'))
            published = pymysql.escape_string(cleanup_published(data=entry.get('published')))
            summary = cleanup_summary(pymysql.escape_string(entry.get('summary')))
            title = pymysql.escape_string(entry.get('title'))
            tags = cleanup_tags(data=entry.get('tags'))

            # Add job2tag to db.
            for tag in tags:
                db.execute(insert_job2tags.format(jobnumber, pymysql.escape_string(tag)))

            # Add job to db.
            db.execute(insert_job.format(jobnumber, link, published, summary, title, location))


if __name__ == '__main__':
    logging.basicConfig(level=LOGGING_LEVEL)
    main()
