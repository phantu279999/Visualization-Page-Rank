import os

from _dbconnection.base_sqlite import BaseSqlite


db = BaseSqlite('spider.sqlite')
conn, cur = db.conn, db.cur


def get_nodes_and_max_min_rank(top_nodes):
	cur.execute('''SELECT COUNT(from_id) AS inbound, old_rank, new_rank, id, url 
	    FROM Pages JOIN Links ON Pages.id = Links.to_id
	    WHERE html IS NOT NULL AND ERROR IS NULL
	    GROUP BY id ORDER BY id,inbound''')

	nodes = []
	maxrank = None
	minrank = None
	count = 0
	for row in cur:
		nodes.append(row)
		rank = row[2]
		if maxrank is None or maxrank < rank:
			maxrank = rank
		if minrank is None or minrank > rank:
			minrank = rank
		count += 1
		if count > top_nodes:
			break

	return nodes, maxrank, minrank


def push_data_to_js(nodes, maxrank, minrank):
	fhand = open('spider.js', 'w')
	fhand.write('spiderJson = {"nodes":[\n')
	count = 0
	map, ranks = {}, {}
	for row in nodes:
		if count > 0:
			fhand.write(',\n')
		rank = row[2]
		rank = 19 * ((rank - minrank) / (maxrank - minrank))
		fhand.write('{' + '"weight":' + str(row[0]) + ',"rank":' + str(rank) + ',')
		fhand.write(' "id":' + str(row[3]) + ', "url":"' + row[4] + '"}')
		map[row[3]] = count
		ranks[row[3]] = rank
		count = count + 1
	fhand.write('],\n')

	fhand.write('"links":[\n')
	count = 0
	cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
	for row in cur:
		# print row
		if row[0] not in map or row[1] not in map: continue
		if count > 0: fhand.write(',\n')
		rank = ranks[row[0]]
		srank = 19 * ((rank - minrank) / (maxrank - minrank))
		fhand.write('{"source":' + str(map[row[0]]) + ',"target":' + str(map[row[1]]) + ',"value":3}')
		count = count + 1
	fhand.write(']};')
	fhand.close()


def app_run():
	print("Creating JSON output on spider.js...")
	top_nodes = int(input("How many nodes? "))
	nodes, maxrank, minrank = get_nodes_and_max_min_rank(top_nodes)
	if maxrank == minrank or maxrank is None or minrank is None:
		print("Error - please run sprank.py to compute page rank")
		os._exit(1)
	push_data_to_js(nodes, maxrank, minrank)
	print("Open force.html in a browser to view the visualization")


if __name__ == '__main__':
	app_run()
	cur.close()
