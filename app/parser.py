import csv
import os
import time
import json
import itertools as IT
import multiprocessing as mp

from config import NUM_PROCS, DATA_DIR, DATA_FILE, CSV_DIR, CSV_FILE

# Объединение dict
def union_dict(data):
	res = {}
	for x in data:
		for key1, value1 in x.items():
			id_prod = res.setdefault(key1, {})
			for key2, value2 in value1.items():
				id_prod.setdefault(key2, []).extend(value2)
	
	return res

def worker1(chunk):
	res = {}
	for x in chunk:
		id_prod = res.setdefault(x[0], {})
		id_prod.setdefault(x[2], []).append(x[1])

	return res

# Объединение файлов
def worker2(names):
	res = []
	print(names)

	with open('{}/{}'.format(DATA_DIR, names[0]), 'r') as file1, open('{}/{}'.format(DATA_DIR, names[1]), 'r') as file2:
		r1 = file1.read()
		r2 = file2.read()

		res = union_dict([
			json.loads(r1), 
			json.loads(r2)
		])
		

	with open('{}/{}'.format(DATA_DIR, names[0]), 'w') as file1:
		file1.write(json.dumps(res))

	os.remove('{}/{}'.format(DATA_DIR, names[1]))

	del res

def start():
	time_s = time.time()
	real_cpu = mp.cpu_count()
	num_procs = NUM_PROCS if not NUM_PROCS or NUM_PROCS > real_cpu else real_cpu
	chunksize = 10**5

	pool = mp.Pool(num_procs)
	with open('{}/{}'.format(CSV_DIR, CSV_FILE), 'r') as f:
		reader = csv.reader(f, delimiter=',')
		for i, chunk in enumerate(iter(lambda: list(IT.islice(reader, chunksize*num_procs)), [])):
			chunk = iter(chunk)
			pieces = list(iter(lambda: list(IT.islice(chunk, chunksize)), []))
			result = union_dict(pool.map(worker1, pieces))
			print(i, len(result))

			with open('{}/file{}.json'.format(DATA_DIR, i), 'a') as f:
				f.write(json.dumps(result, separators=(',', ':')))

	pool.close()
	pool.join()

	for i, chunk in enumerate(iter(lambda: list(IT.islice({file_name for file_name in os.listdir("data") if file_name.endswith(".json") and \
					os.stat('{}/{}'.format(DATA_DIR, file_name)).st_size != 0}, num_procs * 2)), [])):

		if len(chunk) == 1:
			os.rename('{}/{}'.format(DATA_DIR, chunk[0]),'{}/{}'.format(DATA_DIR, DATA_FILE))
			break

		elif len(chunk) % 2 != 0:
			chunk = chunk[:-1]

		chunk = iter(chunk)
		pieces = list(iter(lambda: list(IT.islice(chunk, 2)), []))
		with mp.Pool(processes=len(pieces)) as pool:
			result = pool.map(worker2, pieces)

	time_e = time.time()
	print(time_e - time_s)