from _dbconnection.base_sqlite import BaseSqlite


def app_run():
	db = BaseSqlite('spider.sqlite')
	_data = db.query_all('''SELECT COUNT(from_id) AS inbound, old_rank, new_rank, id, url 
		 FROM Pages JOIN Links ON Pages.id = Links.to_id
		 WHERE html IS NOT NULL
		 GROUP BY id ORDER BY inbound DESC''')

	count = 0
	for row in _data:
		if count < 50:
			print(row)
		count += 1
	print(count, 'rows.')

	db.close_db()


if __name__ == '__main__':
	app_run()
