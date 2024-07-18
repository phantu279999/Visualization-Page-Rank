import os
import sqlite3

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()


def get_list_fromids():
	# Find the ids that send out page rank - we only are interested
	# in pages in the SCC that have in and out links
	cur.execute('''SELECT DISTINCT from_id FROM Links''')
	from_ids = []
	for row in cur:
		from_ids.append(row[0])
	return from_ids


def get_toids_and_links(from_ids):
	# Find the ids that receive page rank
	to_ids = []
	links = []
	cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
	for row in cur:
		from_id, to_id = row[0], row[1]
		if from_id == to_id:
			continue
		if to_id not in from_ids:
			continue

		links.append(row)
		if to_id not in to_ids:
			to_ids.append(to_id)

	return to_ids, links


def get_current_pagerank(from_ids):
	# Get latest page ranks for strongly connected component
	prev_ranks = {}
	for node in from_ids:
		cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node,))
		row = cur.fetchone()
		prev_ranks[node] = row[0]

	return prev_ranks


def calculator_pagerank(prev_ranks, links, to_ids, many):
	# Lets do Page Rank in memory so it is really fast
	for i in range(many):
		next_ranks = {}
		total = 0.0
		for (node, old_rank) in list(prev_ranks.items()):
			total += old_rank
			next_ranks[node] = 0.0
		# print(total)
		# Find the number of outbound links and sent the page rank down each
		for (node, old_rank) in list(prev_ranks.items()):
			# print(node, old_rank)
			give_ids = []
			for from_id, to_id in links:
				if (from_id != node) or (to_id not in to_ids):
					continue
				give_ids.append(to_id)

			if len(give_ids) < 1:
				continue
			amount = old_rank / len(give_ids)
			# print(node, old_rank, amount, give_ids)

			for id in give_ids:
				next_ranks[id] = next_ranks[id] + amount

		new_total = 0
		for (node, next_rank) in list(next_ranks.items()):
			new_total = new_total + next_rank
		evap = (total - new_total) / len(next_ranks)

		# print(new_total, evap)
		for node in next_ranks:
			next_ranks[node] = next_ranks[node] + evap

		# Compute the per-page average change from old rank to new rank
		# As indication of convergence of the algorithm
		total_diff = 0
		for node, old_rank in list(prev_ranks.items()):
			new_rank = next_ranks[node]
			diff = abs(old_rank - new_rank)
			total_diff += diff

		average_diff = total_diff / len(prev_ranks)
		print(i + 1, average_diff)

		# rotate
		prev_ranks = next_ranks

	# Put the final ranks back into the database
	# print(prev_ranks)
	# print(links)
	# print(to_ids)
	cur.execute('''UPDATE Pages SET old_rank=new_rank''')
	for id, new_rank in list(prev_ranks.items()):
		cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
	conn.commit()


def app_run():
	from_ids = get_list_fromids()
	to_ids, links = get_toids_and_links(from_ids)
	prev_ranks = get_current_pagerank(from_ids)

	# Sanity check
	if len(prev_ranks.keys()) < 1:
		print("Nothing to page rank. Check data again.")
		os._exit(1)

	sval = input('How many iterations:')
	many = 1
	if len(sval) > 0:
		many = int(sval)
	calculator_pagerank(prev_ranks, links, to_ids, many)


if __name__ == '__main__':
	app_run()
	cur.close()
