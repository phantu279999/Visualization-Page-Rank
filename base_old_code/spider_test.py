import sqlite3
import traceback
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('../spider.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

# Check to see if we are already in progress...
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()
if row is not None:
	print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
else:
	starturl = input('Enter web url or enter: ')
	if len(starturl) < 1:
		starturl = 'http://www.dr-chuck.com/'
	if starturl.endswith('/'):
		starturl = starturl[:-1]
	web = starturl
	if (starturl.endswith('.htm') or starturl.endswith('.html')):
		pos = starturl.rfind('/')
		web = starturl[:pos]

	if len(web) > 1:
		cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web,))
		cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (starturl,))
		conn.commit()

# Get the current webs
cur.execute('''SELECT url FROM Webs''')
webs = list()
for row in cur:
	webs.append(str(row[0]))

print(webs)

many = 0
while True:
	if many < 1:
		sval = input('How many pages:')
		if len(sval) < 1:
			break
		many = int(sval)
	many = many - 1

	cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
	try:
		row = cur.fetchone()
		fromid = row[0]
		url = row[1]
	except:
		# print(traceback.format_exc())
		print('No unretrieved HTML pages found')
		continue
		# many = 0
		# break

	print(fromid, url, end=' ')

	# If we are retrieving this page, there should be no links from it
	cur.execute('DELETE from Links WHERE from_id=?', (fromid,))
	try:
		document = urlopen(url, context=ctx)
		html = document.read()
		if document.getcode() != 200:
			print("Error on page: ", document.getcode())
			cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))

		if 'text/html' != document.info().get_content_type():
			print("Ignore non text/html page")
			cur.execute('DELETE FROM Pages WHERE url=?', (url,))
			conn.commit()
			continue

		print('(' + str(len(html)) + ')', end=' ')

		soup = BeautifulSoup(html, "html.parser")
	except KeyboardInterrupt:
		print('')
		print('Program interrupted by user...')
		break
	except:
		print(traceback.format_exc())
		print("Unable to retrieve or parse page")
		cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
		conn.commit()
		continue

	cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (url,))
	cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url))
	conn.commit()

	# Retrieve all of the anchor tags
	tags = soup('a')
	count = 0
	for tag in tags:
		href = tag.get('href', None)
		if href is None:
			continue
		print("===========", href)
		# Resolve relative references like href="/contact"
		up = urlparse(href)
		if len(up.scheme) < 1:
			href = urljoin(url, href)
		ipos = href.find('#')
		if ipos > 1:
			href = href[:ipos]
		if href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif'):
			continue
		if href.endswith('/'):
			href = href[:-1]
		# print(href)
		if len(href) < 1: continue

		# Check if the URL is in any of the webs
		found = False
		for web in webs:
			print("---------", href.startswith(web))
			print(href, web)
			if href.startswith(web):
				found = True
				break
		if not found: continue

		cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (href,))
		count = count + 1
		conn.commit()

		cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (href,))
		try:
			row = cur.fetchone()
			toid = row[0]
		except:
			print('Could not retrieve id')
			continue
		# print(fromid, toid)
		cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))

	print(count)

cur.close()



# db = BaseSqlite('spider.sqlite')
# conn, cur = db.conn, db.cur
#
#
# def setup_certificate():
# 	# Ignore SSL certificate errors
# 	ctx = ssl.create_default_context()
# 	ctx.check_hostname = False
# 	ctx.verify_mode = ssl.CERT_NONE
# 	return ctx
#
#
# def setup_table():
# 	cur.execute('''CREATE TABLE IF NOT EXISTS Pages
# 		(id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
# 		 error INTEGER, old_rank REAL, new_rank REAL)''')
#
# 	cur.execute('''CREATE TABLE IF NOT EXISTS Links
# 		(from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')
#
# 	cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')
#
#
# def setup_web_crawl(starturl=None):
# 	# Check to see if we are already in progress...
# 	cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
# 	row = cur.fetchone()
# 	if row is not None:
# 		print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
# 	else:
# 		if starturl is None:
# 			starturl = input('Enter web url or enter(default web): ')
# 			if len(starturl) < 1:
# 				starturl = 'http://www.dr-chuck.com/'
# 		starturl = starturl.strip("/")
# 		web = starturl
# 		if starturl.endswith('.htm') or starturl.endswith('.html'):
# 			pos = starturl.rfind('/')
# 			web = starturl[:pos]
#
# 		if len(web) > 1:
# 			cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web,))
# 			cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (starturl,))
# 			conn.commit()
#
#
# def get_webs_crawl():
# 	# Get the current webs
# 	cur.execute('SELECT url FROM Webs')
# 	webs = list()
# 	for row in cur:
# 		webs.append(str(row[0]))
#
# 	return webs
#
#
# def get_list_page(top=100):
# 	cur.execute(
# 		'SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT {}'.format(top)
# 	)
# 	url_list = []
# 	for it in cur.fetchall():
# 		fromid = it[0]
# 		url = it[1]
# 		print("----", fromid, url)
# 		url_list.append([fromid, url])
#
# 	return url_list
#
#
# def get_text_and_document_html(url):
# 	document = urlopen(url, context=setup_certificate())
# 	html = document.read()
# 	if document.getcode() != 200:
# 		print("Error on page: ", document.getcode())
# 		return False, document
# 	if 'text/html' != document.info().get_content_type():
# 		print("Ignore non text/html page")
# 		return None, document
#
# 	print('(' + str(len(html)) + ')')
# 	return html, document
#
#
# def get_list_link_in_page(webs, url, soup):
# 	tags = soup('a', href=True)
# 	list_link = []
# 	for tag in tags:
# 		href = tag['href']
# 		if not href.startswith('http'):
# 			href = urljoin(url, href)
#
# 		_idx = href.find('#')
# 		if _idx > 1:
# 			href = href[:_idx]
#
# 		href = href.strip("/")
# 		if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')) or (len(href) < 1):
# 			continue
#
# 		# Check if the URL is in any of the webs
# 		found = False
# 		for web in webs:
# 			if href.startswith(web):
# 				found = True
# 				break
#
# 		if found is False:
# 			continue
# 		list_link.append(href)
#
# 	return list_link
#
#
# def process_list_page(webs, list_page):
# 	for fromid, url in list_page:
# 		# If we are retrieving this page, there should be no links from it
# 		cur.execute('DELETE from Links WHERE from_id=?', (fromid,))
# 		try:
# 			html, document = get_text_and_document_html(url)
# 			if html is False:
# 				cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))
#
# 			if html is None:
# 				cur.execute('DELETE FROM Pages WHERE url=?', (url,))
# 				conn.commit()
# 				continue
# 			soup = BeautifulSoup(html, "html.parser")
# 		except KeyboardInterrupt:
# 			# Condition Ctrl+C,...
# 			print('Program interrupted by user...')
# 			break
# 		except Exception as ex:
# 			print("Unable to retrieve or parse page:", ex)
# 			cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
# 			conn.commit()
# 			continue
#
# 		cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (url,))
# 		cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url))
# 		conn.commit()
#
# 		list_link = get_list_link_in_page(webs, url, soup)
# 		for link in list_link:
# 			cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (link,))
# 			conn.commit()
#
# 			cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (link,))
# 			try:
# 				row = cur.fetchone()
# 				toid = row[0]
# 			except Exception as ex:
# 				print('Could not retrieve id:', ex)
# 				continue
# 			cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))
#
# 		print("----", fromid, url, len(list_link))
#
#
# def crawl_html_to_link(top=100):
# 	cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT {}'.format(top))
#
# 	for it in cur.fetchall():
# 		fromid = it[0]
# 		url = it[1]
# 		cur.execute('DELETE from Links WHERE from_id=?', (fromid,))
# 		try:
# 			document = urlopen(url, context=setup_certificate())
# 			html = document.read()
# 			if document.getcode() != 200:
# 				print("Error on page: ", document.getcode())
# 				cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))
#
# 			if 'text/html' != document.info().get_content_type():
# 				print("Ignore non text/html page")
# 				cur.execute('DELETE FROM Pages WHERE url=?', (url,))
# 				conn.commit()
# 				continue
#
# 			print('(' + str(len(html)) + ')', url)
# 		except KeyboardInterrupt:
# 			print('')
# 			print('Program interrupted by user...')
# 			break
# 		except:
# 			print(traceback.format_exc())
# 			print("Unable to retrieve or parse page")
# 			cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
# 			conn.commit()
# 			continue
#
# 		cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (url,))
# 		cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url))
# 		conn.commit()
#
# 	cur.close()
#
#
# def app_run():
# 	web_crawl = 'https://soha.vn/'
# 	top = 10
# 	setup_table()
# 	setup_web_crawl(web_crawl)
# 	webs = get_webs_crawl()
# 	print(webs)
# 	list_page = get_list_page(top)
# 	process_list_page(webs, list_page)
# 	# crawl_html_to_link()