import sqlite3


class BaseSqlite:

	def __init__(self, name_db):
		self.conn = sqlite3.connect(name_db)
		self.cur = self.conn.cursor()

	def query_all(self, query):
		self.cur.execute(query)
		res = []
		for row in self.cur:
			res.append(row)
		return res

	def query_one(self, query):
		self.cur.execute(query)
		res = []
		for row in self.cur:
			res.append(row)
			break
		return res

	def execute(self, query):
		self.cur.execute(query)
		self.commit()

	def commit(self):
		self.conn.commit()

	def close_db(self):
		self.cur.close()

