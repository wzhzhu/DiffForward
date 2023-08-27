import time
import os
import pandas as pd

start_time = time.time()
buffer_space_record = list()
while True:
    result = os.popen('df -Th| grep buffer_space').readlines()[0].split()[3]
    buffer_space_record.append(result)
    time.sleep(5)
    if time.time() - start_time >= 60*140:
        break
df = pd.DataFrame({'buffer space':buffer_space_record})
df.to_csv('/home/k8s/wzhzhu/buffer_space_record.csv')