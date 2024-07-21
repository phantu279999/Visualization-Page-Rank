import traceback
import ssl
from urllib.parse import urljoin
from urllib.request import urlopen
from bs4 import BeautifulSoup

from _dbconnection.base_sqlite import BaseSqlite


class SpiderWeb:
	def __init__(self):
		self.db = BaseSqlite('spider.sqlite')
		self.conn = self.db.conn
		self.cur = self.db.cur

	@staticmethod
	def setup_certificate():
		# Ignore SSL certificate errors
		ctx = ssl.create_default_context()
		ctx.check_hostname = False
		ctx.verify_mode = ssl.CERT_NONE
		return ctx

	def setup_table(self):
		self.cur.execute('''CREATE TABLE IF NOT EXISTS Pages (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
			error INTEGER, old_rank REAL, new_rank REAL)''')

		self.cur.execute('''CREATE TABLE IF NOT EXISTS Links (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')

		self.cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

	def setup_web_crawl(self, starturl=None):
		# Check to see if we are already in progress...
		self.cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
		row = self.cur.fetchone()
		if row is not None:
			print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
		else:
			if starturl is None:
				starturl = input('Enter web url or enter(default web): ')
				if len(starturl) < 1:
					starturl = 'http://www.dr-chuck.com/'
			starturl = starturl.strip("/")
			web = starturl
			if starturl.endswith('.htm') or starturl.endswith('.html'):
				pos = starturl.rfind('/')
				web = starturl[:pos]

			if len(web) > 1:
				self.cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web,))
				self.cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (starturl,))
				self.conn.commit()

	def get_webs_crawl(self):
		# Get the current webs
		self.cur.execute('SELECT url FROM Webs')
		webs = []
		for row in self.cur:
			webs.append(str(row[0]))

		return webs

	def get_list_page(self, top=100):
		self.cur.execute(
			'SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT {}'.format(top)
		)
		url_list = []
		for it in self.cur.fetchall():
			fromid = it[0]
			url = it[1]
			print("----", fromid, url)
			url_list.append([fromid, url])

		return url_list

	def get_text_and_document_html(self, url):
		document = urlopen(url, context=self.setup_certificate())
		html = document.read()
		if document.getcode() != 200:
			print("Error on page: ", document.getcode())
			return False, document
		if 'text/html' != document.info().get_content_type():
			print("Ignore non text/html page")
			return None, document

		print('(' + str(len(html)) + ')')
		return html, document

	def get_list_link_in_page(self, webs, url, soup):
		tags = soup('a', href=True)
		list_link = []
		for tag in tags:
			href = tag['href']
			if not href.startswith('http'):
				href = urljoin(url, href)

			_idx = href.find('#')
			if _idx > 1:
				href = href[:_idx]

			href = href.strip("/")
			if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')) or (len(href) < 1):
				continue

			# Check if the URL is in any of the webs
			found = False
			for web in webs:
				if href.startswith(web):
					found = True
					break

			if found is False:
				continue
			list_link.append(href)

		return list_link

	def process_list_page(self, webs, list_page):
		for fromid, url in list_page:
			# If we are retrieving this page, there should be no links from it
			self.cur.execute('DELETE from Links WHERE from_id=?', (fromid,))
			try:
				html, document = self.get_text_and_document_html(url)
				if html is False:
					self.cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))
				if html is None:
					self.cur.execute('DELETE FROM Pages WHERE url=?', (url,))
					self.conn.commit()
					continue
				soup = BeautifulSoup(html, "html.parser")
			except KeyboardInterrupt:
				# Condition Ctrl+C,...
				print('Program interrupted by user...')
				break
			except Exception as ex:
				print("Unable to retrieve or parse page:", ex)
				self.cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
				self.conn.commit()
				continue

			self.cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (url,))
			self.cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url))
			self.conn.commit()

			list_link = self.get_list_link_in_page(webs, url, soup)
			for link in list_link:
				self.cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (link,))
				self.conn.commit()

				self.cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (link,))
				try:
					row = self.cur.fetchone()
					toid = row[0]
				except Exception as ex:
					print('Could not retrieve id:', ex)
					continue
				self.cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))

			print("----", fromid, url, len(list_link))

	def app_run(self):
		web_crawl = 'https://soha.vn/'
		top = 10
		self.setup_table()
		self.setup_web_crawl(web_crawl)
		webs = self.get_webs_crawl()
		print(webs)
		list_page = self.get_list_page(top)
		self.process_list_page(webs, list_page)
		self.db.close_db()

	def crawl_html_to_link(self, top=100):
		"""
			This is for case want to push data html to Pages
		"""
		self.cur.execute(
			'SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT {}'.format(top))

		for it in self.cur.fetchall():
			fromid = it[0]
			url = it[1]
			self.cur.execute('DELETE from Links WHERE from_id=?', (fromid,))
			try:
				document = urlopen(url, context=self.setup_certificate())
				html = document.read()
				if document.getcode() != 200:
					print("Error on page: ", document.getcode())
					self.cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))

				if 'text/html' != document.info().get_content_type():
					print("Ignore non text/html page")
					self.cur.execute('DELETE FROM Pages WHERE url=?', (url,))
					self.conn.commit()
					continue

				print('(' + str(len(html)) + ')', url)
			except KeyboardInterrupt:
				print('')
				print('Program interrupted by user...')
				break
			except:
				print(traceback.format_exc())
				print("Unable to retrieve or parse page")
				self.cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
				self.conn.commit()
				continue

			self.cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', (url,))
			self.cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url))
			self.conn.commit()

		self.cur.close()


if __name__ == '__main__':
	spider = SpiderWeb()
	spider.app_run()
