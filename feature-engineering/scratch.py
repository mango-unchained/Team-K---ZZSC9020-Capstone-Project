from datetime import datetime
import pandas as pd
import pytz
from astral import LocationInfo
from astral.sun import sun

STATE_TIMEZONES = {
    'NSW': 'Australia/Sydney',
    'QLD': 'Australia/Brisbane',
    'SA': 'Australia/Adelaide',
    'VIC': 'Australia/Melbourne',
}

def is_daylight(utc_datetime: datetime, state: str) -> bool:
    """Determines if a given datetime is during daylight hours in a given Australian state

    Args:
        utc_datetime (datetime): The datetime to check
        state (str): The Australian state to check

    Returns:
        bool: Indicates if the datetime is during daylight hours
    """
    # Convert UTC datetime to local datetime
    local_timezone = pytz.timezone(STATE_TIMEZONES[state])
    local_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(local_timezone)

    city_info = LocationInfo(timezone=STATE_TIMEZONES[state])
    s = sun(city_info.observer, date=local_datetime, tzinfo=local_timezone)
    
    return not (s['sunset'] < local_datetime < s['sunrise'])

df = pd.read_csv('/Users/dsartor/Repos/uni/Team-K---ZZSC9020-Capstone-Project/data/modelling_data.csv')
df["DATETIME"] = pd.to_datetime(df["DATETIME"])
df["is_daylight"] = df.apply(lambda x: is_daylight(x['DATETIME'], x['state']), axis=1)
counts = df["is_daylight"].value_counts()
print(counts)

daylight = df[df['is_daylight'] == True]

print(daylight.head())

not_day = df[df['is_daylight'] == False]

print(not_day.head())
