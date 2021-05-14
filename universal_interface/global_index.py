import fcntl
import os
import time

INDEX_FILE = 'global_index.config'

if not os.path.exists(INDEX_FILE):
    with open(INDEX_FILE, 'w') as f:
        f.write(str(0))


def next_index():
    ret_index, now_index = -1, -1
    file = open(INDEX_FILE, 'r+')
    fcntl.flock(file, fcntl.LOCK_EX)
    now_index = int(file.read())
    file.seek(0)
    ret_index = now_index + 1
    file.write(str(ret_index))
    file.close()
    return ret_index


if __name__ == '__main__':
    # pool = Pool(processes=3)
    s = time.time()
    end_index = -1
    for _ in range(1):
        end_index = next_index()
        # print(pool.apply_async(next_index,).get())
    # pool.close()
    # pool.join()
    print(end_index)
    print((time.time() - s))
