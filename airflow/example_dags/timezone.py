import pendulum

in_utc = pendulum.datetime(2013, 3, 31, 0, 59, 59, fold=0)
tz = pendulum.timezone("Europe/Paris")
in_paris = tz.convert(in_utc)
'2013-03-31T01:59:59+01:00'
print(f'{in_paris = }')
# DateTime(2013, 3, 31, 1, 59, 59, tzinfo=Timezone('Europe/Paris'))

# Shifting time
in_paris = in_paris.add(seconds=1)
print(f'{in_paris = }')
# DateTime(2013, 3, 31, 3, 0, 0, tzinfo=Timezone('Europe/Paris'))

in_paris = in_paris.subtract(seconds=1)
print(f'{in_paris = }')
# DateTime(2013, 3, 31, 1, 59, 59, tzinfo=Timezone('Europe/Paris'))

import datetime

# Normal time (fold=0)
dt1 = datetime.datetime(2023, 11, 5, 1, 30, fold=0)

# Ambiguous time (fold=1)
dt2 = datetime.datetime(2023, 11, 5, 1, 30, fold=1)

print(dt1)
print(dt2)

start_t = pendulum.datetime(2023, 11, 5, 1, 30, tz="Asia/Shanghai")
print(f'{start_t = }')