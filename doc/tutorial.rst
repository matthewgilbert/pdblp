Tutorial on using pdblp
=======================

This tutorial provides some simple use cases for ``pdblp`` . To start with,
import the library and create a ``BCon()`` object

.. ipython::

    In [1]: import pdblp

    In [2]: con = pdblp.BCon(debug=True, port=8194)

Make sure that you are logged in to a Bloomberg terminal, after which you
should be able to to start a connection as follows

.. ipython::

    In [3]: con.start()

To get some historical data, we can call ``bdh()``

.. ipython::

    .. when debug is set to True output is printed to stdout which is
    ..  not picked up so it is necessary to reproduce here
    @verbatim
    In [4]: con.bdh('SPY US Equity', 'PX_LAST',
                    '20150629', '20150630')
    DEBUG:root:Sending Request:
     HistoricalDataRequest = {
        securities[] = {
            "SPY US Equity"
        }
        fields[] = {
            "PX_LAST"
        }
        periodicityAdjustment = ACTUAL
        periodicitySelection = DAILY
        startDate = "20150629"
        endDate = "20150630"
        overrides[] = {
        }
    }
    DEBUG:root:Message Received:
     HistoricalDataResponse = {
        securityData = {
            security = "SPY US Equity"
            eidData[] = {
            }
            sequenceNumber = 0
            fieldExceptions[] = {
            }
            fieldData[] = {
                fieldData = {
                    date = 2015-06-29
                    PX_LAST = 205.420000
                }
                fieldData = {
                    date = 2015-06-30
                    PX_LAST = 205.850000
                }
            }
        }
    }
    Out[4]:
    ticker      SPY US Equity
    2015-06-29         205.42
    2015-06-30         205.85

Notice that when ``con.debug == True`` that the Response and Request messages
are printed to stdout. This can be quite useful for debugging but gets
annoying for normal use, so let's turn it off and get some more data. This time
we request two fields which returns a DataFrame with a MultiIndex by default.

.. ipython:: python

    con.debug = False

    con.bdh('SPY US Equity', ['PX_LAST', 'VOLUME'],
            '20150629', '20150630')

But can also return data in long format

.. ipython:: python

    con.bdh('SPY US Equity', ['PX_LAST', 'VOLUME'],
            '20150629', '20150630', longdata=True)

You can also override different ``FLDS``'s, for example

.. ipython:: python

    con.bdh('MPMIEZMA Index', 'PX_LAST',
            '20150101', '20150830')

    con.bdh('MPMIEZMA Index', 'PX_LAST',
            '20150101', '20150830',
            ovrds=[('RELEASE_STAGE_OVERRIDE', 'P')])

The context can also be managage using ``bopen``

.. ipython:: python

    with pdblp.bopen(port=8194) as bb:
        df = bb.bdh('SPY US Equity', 'PX_LAST',
                    '20150629', '20150630')

The libary also contains functions for accessing reference data, a variety of
usages are shown below

.. ipython:: python

    con.ref('AUDUSD Curncy', 'SETTLE_DT')
    con.ref(['NZDUSD Curncy', 'AUDUSD Curncy'], 'SETTLE_DT')
    con.ref('AUDUSD Curncy', ['SETTLE_DT', 'DAYS_TO_MTY'])
    con.ref(['NZDUSD Curncy', 'AUDUSD Curncy'],
            ['SETTLE_DT', 'DAYS_TO_MTY'])
    con.ref('AUDUSD Curncy', 'SETTLE_DT',
            [('REFERENCE_DATE', '20150715')])
    con.ref(['NZDUSD Curncy', 'AUDUSD Curncy'],
            ['SETTLE_DT', 'DAYS_TO_MTY'],
            [('REFERENCE_DATE', '20150715')])
    con.ref('W 1 Comdty', 'FUT_CHAIN',
            [('INCLUDE_EXPIRED_CONTRACTS', 'Y')]).head()

There are some types of reference data which cannot be downloaded in batch
but support overriding the reference date. For this type of data, ``ref_hist()``
is useful to sequentially override the reference date to generate a time
series. A word of caution, under the hood this is making a number of
``ReferenceDataRequest`` s and thus can throttle your daily data limits if
queried over large date ranges.

.. ipython:: python

    con.ref_hist('AUD1M Curncy', 'DAYS_TO_MTY',
                 dates=['20150625', '20150626'])
    con.ref_hist(['AUD1M Curncy', 'NZD1M Curncy'],
                  'DAYS_TO_MTY',
                  dates=['20150625', '20150626'])
    con.ref_hist('AUD1M Curncy', ['DAYS_TO_MTY', 'SETTLE_DT'],
                 dates=['20150625', '20150626'])
    con.ref_hist(['AUD1M Curncy', 'NZD1M Curncy'],
                 ['DAYS_TO_MTY', 'SETTLE_DT'],
                 dates=['20150625', '20150626'])
    con.ref_hist(['AUD1M Curncy', 'NZD1M Curncy'],
                 ['DAYS_TO_MTY', 'SETTLE_DT'],
                 dates=['20150625', '20150626'])
    con.ref_hist("BVIS0587 Index", "CURVE_TENOR_RATES",
                 dates=['20160625'],
                 date_field="CURVE_DATE").head()

A useful trick to avoid throttling your connection when querying large data or
to ensure you can reproduce your results without a connection in the future is
to make use of the excellent ``joblib`` library. For example

.. ipython:: python

    import joblib
    import shutil
    from tempfile import mkdtemp
    temp_dir = mkdtemp()
    cacher = joblib.Memory(temp_dir)
    bdh = cacher.cache(con.bdh, ignore=['self'])
    bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630')
    bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630')
    shutil.rmtree(temp_dir)

You can also access Bloomberg SRCH data using ``bsrch``

.. ipython:: python

    con.bsrch("COMDTY:VESSEL").head()
