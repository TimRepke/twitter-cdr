import datetime
from enum import Enum
from dataclasses import dataclass
from collections import OrderedDict


class EventType(Enum):
    COP = 'COP'
    UN = 'UN'
    NAT = 'NAT'  # natural disaster
    PAN = 'PAN'  # covid pandemic
    IPC = 'IPC'  # IPCC reports
    MISC = 'MISC'  # other things


@dataclass
class Event:
    name: str
    type: EventType
    date_start: datetime.date
    date_end: datetime.date | None = None  # only set if this is a multi-day event


events: dict[str, Event] = OrderedDict([
    ('twitter', Event(name='Twitter was founded', type=EventType.MISC,  # https://en.wikipedia.org/wiki/Twitter
                      date_start=datetime.date(2007, 3, 21))),
    ('cop13', Event(name='COP13 Bali', type=EventType.COP,  # https://unfccc.int/event/cop-13
                    date_start=datetime.date(2007, 12, 3), date_end=datetime.date(2007, 12, 14))),
    ('cop14', Event(name='COP14 Poznan', type=EventType.COP,  # https://unfccc.int/event/cop-14
                    date_start=datetime.date(2008, 12, 1), date_end=datetime.date(2008, 12, 12))),
    ('cop15', Event(name='COP15 Copenhagen', type=EventType.COP,  # https://unfccc.int/event/cop-15
                    date_start=datetime.date(2009, 12, 7), date_end=datetime.date(2009, 12, 18))),
    ('cop16', Event(name='COP16 Cancun', type=EventType.COP,  # https://unfccc.int/event/cop-16
                    date_start=datetime.date(2010, 11, 29), date_end=datetime.date(2010, 12, 10))),
    ('cop17', Event(name='COP17 Durban', type=EventType.COP,  # https://unfccc.int/event/cop-17
                    date_start=datetime.date(2011, 11, 28), date_end=datetime.date(2011, 12, 9))),
    ('ar4_1', Event(name='AR4 WGI release', type=EventType.IPC,
                    date_start=datetime.date(2007, 3, 1))),
    ('ar4_3', Event(name='AR4 WGIII release', type=EventType.IPC,
                    date_start=datetime.date(2007, 9, 1))),
    ('ar4_2', Event(name='AR4 WGII release', type=EventType.IPC,
                    date_start=datetime.date(2007, 9, 18))),
    ('cop18', Event(name='COP18 Doha', type=EventType.COP,
                    date_start=datetime.date(2012, 11, 26), date_end=datetime.date(2012, 12, 8))),
    ('cop19', Event(name='COP19 Warsaw', type=EventType.COP,
                    date_start=datetime.date(2013, 11, 11), date_end=datetime.date(2013, 11, 23))),
    ('cop20', Event(name='COP20 Lima', type=EventType.COP,
                    date_start=datetime.date(2014, 12, 1), date_end=datetime.date(2014, 12, 12))),
    ('ar5_1', Event(name='AR5 WGI release', type=EventType.IPC,
                    date_start=datetime.date(2013, 9, 30))),
    ('ar5_2', Event(name='AR5 WGII release', type=EventType.IPC,
                    date_start=datetime.date(2014, 3, 31))),
    ('ar5_3', Event(name='AR5 WGIII release', type=EventType.IPC,
                    date_start=datetime.date(2014, 4, 15))),
    ('cop21', Event(name='COP21 Paris', type=EventType.COP,
                    date_start=datetime.date(2015, 11, 30), date_end=datetime.date(2015, 12, 12))),
    ('cop22', Event(name='COP22 Marrakech', type=EventType.COP,
                    date_start=datetime.date(2016, 11, 7), date_end=datetime.date(2016, 11, 18))),
    ('harvey', Event(name='Hurricane Harvey', type=EventType.NAT,
                     date_start=datetime.date(year=2017, month=8, day=17))),
    ('cop23', Event(name='COP23 Bonn', type=EventType.COP,
                    date_start=datetime.date(2017, 11, 6), date_end=datetime.date(2017, 11, 17))),
    ('sr15', Event(name='SR15 release', type=EventType.IPC,
                   date_start=datetime.date(2018, 10, 8))),
    ('cop24', Event(name='COP24 Katowice', type=EventType.COP,
                    date_start=datetime.date(2018, 12, 2), date_end=datetime.date(2018, 12, 15))),
    ('srccl', Event(name='SRCCL release', type=EventType.IPC,
                    date_start=datetime.date(2019, 8, 1))),
    ('aus_bush', Event(name='Australian Bushfires', type=EventType.NAT,
                       date_start=datetime.date(year=2019, month=9, day=1))),
    ('srocc', Event(name='SROCC release', type=EventType.IPC,
                    date_start=datetime.date(2019, 9, 2))),
    ('un_cas', Event(name='UN Climate action summit', type=EventType.UN,
                     date_start=datetime.date(2019, 9, 23))),
    ('cop25', Event(name='COP25 Madrid', type=EventType.COP,
                    date_start=datetime.date(2019, 12, 2), date_end=datetime.date(2019, 12, 13))),
    ('cov_spread', Event(name='COVID-19 spreads', type=EventType.PAN,
                         date_start=datetime.date(2019, 12, 18), date_end=datetime.date(2020, 3, 11))),
    ('cov_pan', Event(name='COVID-19 pandemic declared', type=EventType.PAN,
                      date_start=datetime.date(2020, 3, 11))),
    ('ar6_1', Event(name='AR6 WGI release', type=EventType.IPC,
                    date_start=datetime.date(2021, 8, 9))),
    ('ar6_2', Event(name='AR6 WGII release', type=EventType.IPC,
                    date_start=datetime.date(2022, 2, 1))),
    ('ar6_3', Event(name='AR6 WGIII release', type=EventType.IPC,
                    date_start=datetime.date(2022, 4, 1))),
    ('cop26', Event(name='COP26 Glasgow', type=EventType.COP,
                    date_start=datetime.date(2021, 10, 31), date_end=datetime.date(2021, 11, 13))),
    ('cop27', Event(name='COP27 Sharm El Sheikh', type=EventType.COP,
                    date_start=datetime.date(2022, 11, 6), date_end=datetime.date(2022, 11, 20)))
])

cops = OrderedDict([
    (key, value)
    for key, value in events.items()
    if value.type == EventType.COP
])

# ETC Group raises alarm on Ocean Fertilisation Scheme (oct 2012)
# CGG project launch (oct 2012)
# SCoPEx (2015/2017)
# ice911 (2017/2018)
# £800 million CCS Infrastructure Fund released in UK (2020)
