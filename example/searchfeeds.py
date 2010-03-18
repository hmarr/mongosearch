#!/usr/bin/env python

import sys
import os
import time
from operator import itemgetter
from heapq import nlargest
from mongoengine.document import Document
from mongoengine import fields, connect
import feedparser

sys.path.insert(0, '..')

import mongosearch


class BlogPost(Document):
    """A sample blog post document that will be indexed and searched. The title
    is more important than the content so should be weighted higher.
    """
    title = fields.StringField()
    content = fields.StringField()


def get_feed_entries(feed_path):
    """Parse the individual items out of a locally-stored RSS feed.
    """
    document = feedparser.parse(feed_path)

    entries = {}
    for entry in document.entries:
        guid = entry.get('guid') or entry.get('link')
        if guid in entries:
            continue

        # Use content if summary is not present
        summary = entry.get('summary')
        if not summary:
            summary = entry.get('content', [{}])[0].get('value', '')

        entries[guid] = (entry.title, summary)

    return entries

def quit_with_usage():
    print >> sys.stderr, 'Usage: %s <query>' % sys.argv[0]
    sys.exit(1)

def main():
    try:
        query = ' '.join(sys.argv[1:])
    except IndexError:
        quit_with_usage()

    if not query.strip():
        quit_with_usage()

    connect('mongosearch-example')

    # Ensure that no data exists from a previous run of this example
    BlogPost.drop_collection()

    # Create an index for the blog post and add the fields to be indexed
    index = mongosearch.SearchIndex(BlogPost)
    index.add_field('title', html=True, weight=1.5)
    index.add_field('content', html=True)

    # In this example we are loading our test data from downloaded RSS feeds
    # in the 'data' directory
    feeds = ['df.xml', 'register.atom', 'github.xml']
    feed_paths = [os.path.join('fixtures', feed) for feed in feeds]
    for feed_path in feed_paths:
        # Parse the feed and save it to the DB
        entries = get_feed_entries(feed_path)
        for guid, entry in entries.items():
            post = BlogPost(title=entry[0], content=entry[1])
            post.save()

    # Index the collection
    t0 = time.time()
    index.generate_index()
    print 'Indexing took %s seconds' % (time.time() - t0)

    # Query the collection
    t0 = time.time()
    results = index.search(query)
    top_matches = nlargest(10, results.iteritems(), itemgetter(1))
    time_taken = time.time() - t0
    print 'Querying took %s seconds' % time_taken

    # Write the results to results.htm as HTML
    outfile = open('results.htm', 'w')
    outfile.write('<html><head><style>body{font-size: 70%;}</style>')
    outfile.write('<meta http-equiv="Content-Type" content="text/html; ')
    outfile.write('charset=UTF-8"/>')
    outfile.write('</head><body>')
    outfile.write('<h1>Search results for "%s"</h1>' % query)
    outfile.write('<p><em>Query took %s seconds</em></p>' % time_taken)
    for doc_id, score in top_matches:
        doc = BlogPost.objects(id=doc_id).first()
        outfile.write('<h2>[%s] %s</h2>' % (score, doc.title.encode('utf8')))
        outfile.write('<p>%s</p>' % doc.content.encode('utf8'))
        outfile.write('<br />')
    outfile.write('</body></html>')

    print 'Processed %s items' % BlogPost.objects.count()

    print 'Results saved in results.htm'

if __name__ == '__main__':
    main()
