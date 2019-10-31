with open('temp.csv', 'r') as f_in, open('2019_precipitation.csv', 'w') as f_out:
    f_out.write(next(f_in))
    [f_out.write(','.join(line.split()) + '\n') for line in f_in]