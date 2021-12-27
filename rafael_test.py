import pandas as pd
import numpy as np
import csv
import time

N_ROWS = 20000
STEP = 2000


def step_1():
    start_time = time.time()
    df = pd.read_csv('sample.csv')
    sorted_df = df.sort_values(by=['data'])
    sorted_df.set_index('row_num').to_csv('sorting-step1.csv')
    end_time = time.time()
    with open('sorting-step1_process_time.txt', 'w') as time_1:
        time_1.write(f'time took for step 1: {end_time - start_time}')


def step_2():
    start_time = time.time()
    columns = list(pd.read_csv('sample.csv', nrows=3).columns)
    for i in range(1, N_ROWS, STEP):
        data = pd.read_csv('sample.csv', skiprows=i, nrows=STEP, names=columns)
        sorted_df = data.sort_values(by=['data'])
        sorted_df.set_index('row_num').to_csv(f'data_{i}.csv')

    small_step = int(STEP / len(range(0, N_ROWS, STEP)))
    for j in range(1, STEP, small_step):
        marge = pd.DataFrame(columns=columns)
        for i in range(1, N_ROWS, STEP):
            d = pd.read_csv(f'data_{i}.csv', skiprows=j, nrows=small_step, names=columns)
            marge = marge.append(d)
            sorted_df = marge.sort_values(['data'])
            sorted_df.set_index('row_num').to_csv('sorting-step2.csv', mode='a')

    end_time = time.time()
    with open('sorting-step2_process_time.txt', 'w') as time_2:
        time_2.write(f'time took for step 2: {end_time - start_time}')


def main():
    step_1()
    step_2()


if __name__ == '__main__':
    main()
