import sqlite3
from _dbconnection.base_sqlite import BaseSqlite


if __name__ == '__main__':
	db = BaseSqlite('spider.sqlite')
	db.execute('''UPDATE Pages SET new_rank=1.0, old_rank=0.0''')
	db.close_db()
	print("All pages set to a rank of 1.0")
