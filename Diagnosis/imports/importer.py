import datetime

import pandas as pd

diagnosis_df = pd.read_csv(r'C:\Users\Acer\Desktop\Sieciechowicz_analytics\Diagnosis\imports\2019_11_04.csv', index_col=False)
mmm2mm = {
    'sty': 1,
    'lut': 2,
    'mar': 3,
    'kwi': 4,
    'maj': 5,
    'cze': 6,
    'lip': 7,
    'sie': 8,
    'wrz': 9,
    'paź': 10,
    'lis': 11,
    'gru': 12,
}
for index, row in diagnosis_df.iterrows():
    when = row.when
    when_parts = when.split(',')
    # print(when_parts)
    when_parts_1 = when_parts[0].split(' ')
    # print(when_parts_1)
    day = when_parts_1[0]
    month = mmm2mm[when_parts_1[1]]
    # print(day)
    print(month)
    when_parts_2 = when_parts[1].split(', ')
    year = when_parts_2[0]
    print(year)

#  Brakuje rok, minuty i godziny, wyciagnac z pozostalych elementow to samo, jak zrobione z month oraz day. Potrzebne sa
# 3 osobne zmienne, year hour minutes (zmienna ma sie nazywac hh, a minuty mm). Na koncu stworzyc obiekt datetime ktory
# ma przyjac argumenty jako zmienne wyciagniete ze stringa.

