import sys, pickle
import pathlib, time
import numpy as np

if len(sys.argv) < 3:
    print('read_relaxation.py matsim_output_path result_path [modes]')
    exit()

source = pathlib.Path(sys.argv[1]).resolve()
destination = sys.argv[2]

modes = sys.argv[3].split(',') if len(sys.argv) > 3 else ['av', 'car', 'pt']

iterations = []
shares = []

display = time.time()
count = 0

print('Reading relaxation over iterations ...\n')

for path in pathlib.Path('%s/ITERS' % source).resolve().iterdir():
    i = int(str(path).split('.')[1])
    iterations.append(i)

    histogram_path = '%s/%d.legHistogram.txt' % (path, i)

    itshares = [0] * len(modes)
    indices = []

    with open(str(histogram_path)) as f:
        for row in f:
            row = row.split('\t')

            if row[0] == 'time':
                indices = [row.index('departures_' + mode) if 'departures_' + mode in row else None for mode in modes]
            else:
                for mi in range(len(modes)):
                    if indices[mi] is not None:
                        itshares[mi] += int(row[indices[mi]])

    shares.append(itshares)
    count += 1

    if display + 1.0 < time.time():
        print('   Read %d iterations ...' % count)
        display = time.time()

shares = np.array(shares)
iterations = np.array(iterations)

data = np.column_stack((iterations, shares))
indices = np.argsort(data[:,0])

data = data[indices,:]

with open(str(destination), 'wb+') as f:
    pickle.dump((data, modes), f)

print('Done reading iterations.')
