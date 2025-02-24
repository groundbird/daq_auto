#!/usr/bin/env python3
'''
Planner that outputs observation timing in UTC
'''
from argparse import ArgumentParser
import numpy as np
import pandas as pd
from astropy import units as u
from astropy.coordinates import AltAz, EarthLocation, Latitude, Longitude, get_body, SkyCoord
from astropy.time import Time, TimeDelta
from datetime import timezone

# Restart timing difference in min.
DF_RISE_MIN = [-15, 0, -60, -45, -15, -45, -30]
DF_SET_MIN = [-45, -60, 0, -15, -45, -15, -30]

# Observatorio del Teide
TO_LAT = Latitude((+28, 18, 00), unit='deg')
TO_LON = Longitude((-16, 30, 35), unit='deg')
TO_HGT = 2390 * u.m
TO = EarthLocation.from_geodetic(lon=TO_LON, lat=TO_LAT, height=TO_HGT)

class BasePredictor:
    ''' Base predictor
    '''
    def __init__(self, body_name, obstime, location=TO):
        try:
            self._body = get_body(body_name, obstime, location=location)
            self._aa = AltAz(location=location)
        except KeyError:
            try:
                self._body = SkyCoord.from_name(body_name)
                self._aa = AltAz(location=location, obstime=obstime)
            except Exception as e:
                raise ValueError(f"Invalid body name '{body_name}': {e}")

        self._body_aa = self._body.transform_to(self._aa)
        self._alt = self._body_aa.alt
        self._az = self._body_aa.az
        self._obstime = self._body_aa.obstime

    def crossing_time(self, alt, return_sign=False):
        ''' Calculate crossing time
        '''
        is_upper_prev = None
        ret_list = []
        sign_list = []
        for i in range(len(self._body_aa)):
            is_upper = self._body_aa[i].alt > alt

            if is_upper_prev is None:
                pass
            else:
                if is_upper == is_upper_prev:
                    pass
                else:
                    alt_prev = self._alt[i-1]
                    alt_curr = self._alt[i]
                    t_off = self._obstime[i-1]
                    t_prev = self._obstime[i-1] - t_off
                    t_curr = self._obstime[i] - t_off
                    t_crossing = (alt_curr - alt)/(alt_curr-alt_prev)*t_prev + \
                                (alt - alt_prev)/(alt_curr-alt_prev)*t_curr + t_off
                    ret_list.append(t_crossing)
                    sign_list.append(alt_prev < alt_curr)

            is_upper_prev = is_upper

        if return_sign:
            return ret_list, sign_list

        return ret_list

def get_plan_oneday(time, line=70, body_name='moon', sun_avoid=60):
    """Plan readout restart timing for observation of specified body.
    
    Args:
        time (astropy.time.Time): Start time of observation
        line (float): Elevation angle of focal plane center (degrees)
        body_name (str): Target body name (e.g., 'moon', 'jupiter')
        sun_avoid (float): Maximum sun elevation during observation (degrees)
    
    Returns:
        pandas.DataFrame: Schedule with columns 'Time (UTC)' and 'Action'
    
    Raises:
        ValueError: If body_name is invalid or time is inappropriate
        Exception: For other errors during plan generation
    """
    o_times = time + np.linspace(0, 24, 193)*u.hour
    pred_body = BasePredictor(body_name, o_times)
    pred_sun = BasePredictor('sun', o_times)

    crossing_body, sign_body = pred_body.crossing_time(line*u.deg, return_sign=True)
    ## for check
    # current_time = Time.now()
    # time_5min = current_time + TimeDelta(5 * u.min)
    # time_10min = current_time + TimeDelta(10 * u.min)
    # ret_list = [time_5min, time_10min]
    # crossing_body, sign_body = ret_list , [True, False]
    crossing_sun, sign_sun = pred_sun.crossing_time(sun_avoid*u.deg, return_sign=True)

    entries = []

    if len(crossing_body) > 1:
        for cross_time_center, sign in zip(crossing_body, sign_body):
            if sign:
                for det, df_min in enumerate(DF_RISE_MIN):
                    cross_time = cross_time_center + TimeDelta(df_min*u.min)
                    entry = [cross_time.to_datetime(timezone.utc),
                            f'Restart GB{det + 1:02d} for {body_name}-rise.']
                    entries.append(entry)
            else:
                for det, df_min in enumerate(DF_SET_MIN):
                    cross_time = cross_time_center + TimeDelta(df_min*u.min)
                    entry = [cross_time.to_datetime(timezone.utc),
                            f'Restart GB{det + 1:02d} for {body_name}-set.']
                    entries.append(entry)

    for cross_time, sign in zip(crossing_sun, sign_sun):
        if sign:
            entry = [cross_time.to_datetime(timezone.utc),
                    f'Close dome to avoid sun.']
            entries.append(entry)
        else:
            entry = [cross_time.to_datetime(timezone.utc),
                    f'Dome can be opened.']
            entries.append(entry)

    table = pd.DataFrame(entries, columns=['Time (UTC)', 'Action'])
    return table.sort_values('Time (UTC)').reset_index(drop=True)

def main():
    parser = ArgumentParser(description='Generate observation planning table')
    parser.add_argument('body', help='Astronomical body name')
    parser.add_argument('--date', type=str, 
                       help='Observation date (if not specified, uses current time)')
    parser.add_argument('--sun', '-s', type=float, default=60,
                       help='Maximum sun elevation to avoid (default: 60)')
    parser.add_argument('--line', '-l', type=float, default=70,
                       help='Elevation angle of the GB Telescope (default: 70)')
    
    args = parser.parse_args()
    
    try:
        # Convert date string to Time object if provided, otherwise use current time
        time = Time(args.date) if args.date else Time.now()
        
        # Get the plan
        table = get_plan_oneday(time, line=args.line, 
                              body_name=args.body, sun_avoid=args.sun)
        
        # Print formatted table
        print(table.to_string(
            formatters={'Time (UTC)': lambda t: t.strftime("%Y-%m-%d %H:%M")},
            index=False
        ))
        
    except Exception as err:
        print(f"Error: {err}")

if __name__ == '__main__':
    main()
