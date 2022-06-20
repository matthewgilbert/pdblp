import pytest
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal, assert_index_equal
from pdblp import pdblp
import blpapi
import os


@pytest.fixture(scope="module")
def port(request):
    return request.config.getoption("--port")


@pytest.fixture(scope="module")
def host(request):
    return request.config.getoption("--host")


@pytest.fixture(scope="module")
def timeout(request):
    return request.config.getoption("--timeout")


@pytest.fixture(scope="module")
def con(host, port, timeout):
    return pdblp.BCon(host=host, port=port, timeout=timeout).start()


@pytest.fixture(scope="module")
def data_path():
    return os.path.join(os.path.dirname(__file__), "data/")


def pivot_and_assert(df, df_exp, with_date=False):
    # as shown below, since the raw data returned from bbg is an array
    # with unknown ordering, there is no guruantee that the `position` will
    # always be the same so pivoting prior to comparison is necessary
    #
    # fieldData = {
    #     INDX_MWEIGHT[] = {
    #         INDX_MWEIGHT = {
    #             Member Ticker and Exchange Code = "BON8"
    #             Percentage Weight = 2.410000
    #         }
    #         INDX_MWEIGHT = {
    #             Member Ticker and Exchange Code = "C N8"
    #             Percentage Weight = 6.560000
    #         }
    #         INDX_MWEIGHT = {
    #             Member Ticker and Exchange Code = "CLN8"
    #             Percentage Weight = 7.620000
    #         }
    #     }
    # }
    name_cols = list(df_exp.name.unique())
    sort_cols = list(df_exp.name.unique())
    index_cols = ["name", "position", "field", "ticker"]
    if with_date:
        sort_cols.append("date")
        index_cols.append("date")

    df = (df.set_index(index_cols).loc[:, "value"]
          .unstack(level=0).reset_index().drop(columns="position")
          .sort_values(by=sort_cols, axis=0))
    df_exp = (df_exp.set_index(index_cols).loc[:, "value"]
              .unstack(level=0).reset_index().drop(columns="position")
              .sort_values(by=sort_cols, axis=0))
    # deal with mixed types resulting in str from csv read
    for name in name_cols:
        try:
            df_exp.loc[:, name] = df_exp.loc[:, name].astype(float)
        except ValueError:
            pass
    for name in name_cols:
        try:
            df.loc[:, name] = df.loc[:, name].astype(float)
        except ValueError:
            pass
    if with_date:
        df.loc[:, "date"] = pd.to_datetime(df.loc[:, "date"],
                                           format="%Y%m%d")
        df_exp.loc[:, "date"] = pd.to_datetime(df_exp.loc[:, "date"],
                                               format="%Y%m%d")
    assert_frame_equal(df, df_exp)


ifbbg = pytest.mark.skipif(pytest.config.cache.get('offline', False),
                           reason="No BBG connection, skipping tests")


@ifbbg
def test_bdh_empty_data_only(con):
    df = con.bdh(
            tickers=['1437355D US Equity'],
            flds=['PX_LAST', 'VOLUME'],
            start_date='20180510',
            end_date='20180511',
            longdata=False
    )
    df_exp = pd.DataFrame(
      [], index=pd.DatetimeIndex([], name='date'),
      columns=pd.MultiIndex.from_product([[], []],
                                         names=('ticker', 'field'))
    )
    assert_frame_equal(df, df_exp)


@ifbbg
def test_bdh_empty_data_with_non_empty_data(con):
    df = con.bdh(
            tickers=['AAPL US Equity', '1437355D US Equity'],
            flds=['PX_LAST', 'VOLUME'],
            start_date='20180510',
            end_date='20180511',
            longdata=False
    )
    df_exp = pd.DataFrame(
        [[190.04, 27989289.0], [188.59, 26212221.0]],
        index=pd.DatetimeIndex(["20180510", "20180511"], name="date"),
        columns=pd.MultiIndex.from_product([["AAPL US Equity"],
                                            ["PX_LAST", "VOLUME"]],
                                           names=["ticker", "field"])
    )
    assert_frame_equal(df, df_exp)


@ifbbg
def test_bdh_partially_empty_data(con):
    df = con.bdh(
            tickers=['XIV US Equity', 'AAPL US Equity'],
            flds=['PX_LAST'],
            start_date='20180215',
            end_date='20180216',
            longdata=False
    )
    df_exp = pd.DataFrame(
        [[6.04, 172.99], [np.NaN, 172.43]],
        index=pd.DatetimeIndex(["20180215", "20180216"], name="date"),
        columns=pd.MultiIndex.from_product(
                    [["XIV US Equity", "AAPL US Equity"], ["PX_LAST"]],
                    names=["ticker", "field"]
                )
    )
    assert_frame_equal(df, df_exp)


@ifbbg
def test_bdh_one_ticker_one_field_pivoted(con):
    df = con.bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630')
    midx = pd.MultiIndex(levels=[["SPY US Equity"], ["PX_LAST"]],
                         labels=[[0], [0]], names=["ticker", "field"])
    df_expect = pd.DataFrame(
        index=pd.date_range("2015-06-29", "2015-06-30"),
        columns=midx,
        data=[205.42, 205.85]
    )
    df_expect.index.names = ["date"]
    assert_frame_equal(df, df_expect)


@ifbbg
def test_bdh_one_ticker_one_field_longdata(con):
    df = con.bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630',
                 longdata=True)
    idx = pd.Index(["date", "ticker", "field", "value"])
    data = [["2015-06-29", "2015-06-30"],
            ["SPY US Equity", "SPY US Equity"], ["PX_LAST", "PX_LAST"],
            [205.42, 205.85]]
    df_expect = pd.DataFrame(data=data, index=idx).transpose()
    df_expect.loc[:, "date"] = pd.to_datetime(df_expect.loc[:, "date"])
    df_expect.loc[:, "value"] = np.float64(df_expect.loc[:, "value"])
    assert_frame_equal(df, df_expect)


@ifbbg
def test_bdh_one_ticker_two_field_pivoted(con):
    cols = ['PX_LAST', 'VOLUME']
    df = con.bdh('SPY US Equity', cols, '20150629', '20150630')
    midx = pd.MultiIndex(
        levels=[["SPY US Equity"], cols],
        labels=[[0, 0], [0, 1]], names=["ticker", "field"]
    )
    df_expect = pd.DataFrame(
        index=pd.date_range("2015-06-29", "2015-06-30"),
        columns=midx,
        data=[[205.42, 202621332], [205.85, 182925106]]
    )
    df_expect = df_expect.astype(np.float64)
    df_expect.index.names = ["date"]
    assert_frame_equal(df, df_expect)


@ifbbg
def test_bdh_one_ticker_two_field_longdata(con):
    cols = ['PX_LAST', 'VOLUME']
    df = con.bdh('SPY US Equity', cols, '20150629', '20150630',
                 longdata=True)
    idx = pd.Index(["date", "ticker", "field", "value"])
    data = [["2015-06-29", "2015-06-29", "2015-06-30", "2015-06-30"],
            ["SPY US Equity", "SPY US Equity", "SPY US Equity", "SPY US Equity"],  # NOQA
            ["PX_LAST", "VOLUME", "PX_LAST", "VOLUME"],
            [205.42, 202621332, 205.85, 182925106]]
    df_expect = pd.DataFrame(data=data, index=idx).transpose()
    df_expect.loc[:, "date"] = pd.to_datetime(df_expect.loc[:, "date"])
    df_expect.loc[:, "value"] = np.float64(df_expect.loc[:, "value"])
    assert_frame_equal(df, df_expect)


@ifbbg
def test_bdh_value_errors(con):
    bad_col = "not_a_fld"
    with pytest.raises(ValueError):
        con.bdh("SPY US Equity", bad_col, "20150630", "20150630")

    bad_ticker = "not_a_ticker"
    with pytest.raises(ValueError):
        con.bdh(bad_ticker, "PX_LAST", "20150630", "20150630")


@ifbbg
def test_bdib(con):
    # BBG has limited history for the IntradayBarRequest service so request
    # recent data
    prev_busday = pd.Timestamp(
        pd.np.busday_offset(pd.Timestamp.today().date(), -1)
    )
    ts1 = prev_busday.strftime("%Y-%m-%d") + "T10:00:00"
    ts2 = prev_busday.strftime("%Y-%m-%d") + "T10:20:01"
    df = con.bdib('SPY US Equity', ts1, ts2, event_type="BID", interval=10)

    ts2e = prev_busday.strftime("%Y-%m-%d") + "T10:20:00"
    idx_exp = pd.date_range(ts1, ts2e, periods=3, name="time")
    col_exp = pd.Index(["open", "high", "low", "close", "volume", "numEvents"])

    assert_index_equal(df.index, idx_exp)
    assert_index_equal(df.columns, col_exp)


# REF TESTS
@ifbbg
def test_ref_one_ticker_one_field(con):
    df = con.ref('AUD Curncy', 'NAME')
    df_expect = pd.DataFrame(
        columns=["ticker", "field", "value"],
        data=[["AUD Curncy", "NAME", "Australian Dollar Spot"]]
    )
    assert_frame_equal(df, df_expect)


@ifbbg
def test_ref_one_ticker_one_field_override(con):
    df = con.ref('AUD Curncy', 'SETTLE_DT',
                 [("REFERENCE_DATE", "20161010")])
    df_expect = pd.DataFrame(
        columns=["ticker", "field", "value"],
        data=[["AUD Curncy", "SETTLE_DT",
              pd.datetime(2016, 10, 12).date()]]
    )
    assert_frame_equal(df, df_expect)


@ifbbg
def test_ref_invalid_field(con):
    with pytest.raises(ValueError):
        con.ref("EI862261 Corp", "not_a_field")


@ifbbg
def test_ref_not_applicable_field(con):
    # test both cases described in
    # https://github.com/matthewgilbert/pdblp/issues/6
    df = con.ref("BCOM Index", ["INDX_GWEIGHT"])
    df_expect = pd.DataFrame(
        [["BCOM Index", "INDX_GWEIGHT", np.NaN]],
        columns=['ticker', 'field', 'value']
    )
    assert_frame_equal(df, df_expect)

    df = con.ref("BCOM Index", ["INDX_MWEIGHT_PX2"])
    df_expect = pd.DataFrame(
        [["BCOM Index", "INDX_MWEIGHT_PX2", np.NaN]],
        columns=['ticker', 'field', 'value']
    )
    assert_frame_equal(df, df_expect)


@ifbbg
def test_ref_invalid_security(con):
    with pytest.raises(ValueError):
        con.ref("NOT_A_TICKER", "MATURITY")


@ifbbg
def test_ref_applicable_with_not_applicable_field(con):
    df = con.ref("BVIS0587 Index", ["MATURITY", "NAME"])
    df_exp = pd.DataFrame(
        [["BVIS0587 Index", "MATURITY", np.NaN],
         ["BVIS0587 Index", "NAME", "CAD Canada Govt BVAL Curve"]],
        columns=["ticker", "field", "value"])
    assert_frame_equal(df, df_exp)


@ifbbg
def test_ref_mixed_data_error(con):
    # calling ref which returns singleton and array data throws error
    with pytest.raises(ValueError):
        con.ref('CL1 Comdty', 'FUT_CHAIN')


# BULKREF TESTS
@ifbbg
def test_bulkref_one_ticker_one_field(con, data_path):
    df = con.bulkref('BCOM Index', 'INDX_MWEIGHT',
                     ovrds=[("END_DATE_OVERRIDE", "20150530")])
    df_expected = pd.read_csv(
        os.path.join(data_path, "bulkref_20150530.csv")
    )
    pivot_and_assert(df, df_expected)


@ifbbg
def test_bulkref_two_ticker_one_field(con, data_path):
    df = con.bulkref(['BCOM Index', 'OEX Index'], 'INDX_MWEIGHT',
                     ovrds=[("END_DATE_OVERRIDE", "20150530")])
    df_expected = pd.read_csv(
        os.path.join(data_path, "bulkref_two_fields_20150530.csv")
    )
    pivot_and_assert(df, df_expected)


@ifbbg
def test_bulkref_singleton_error(con):
    # calling bulkref which returns singleton throws error
    with pytest.raises(ValueError):
        con.bulkref('CL1 Comdty', 'FUT_CUR_GEN_TICKER')


@ifbbg
def test_bulkref_null_scalar_sub_element(con):
    # related to https://github.com/matthewgilbert/pdblp/issues/32#issuecomment-385555289  # NOQA
    # smoke test to check parse correctly
    ovrds = [("DVD_START_DT", "19860101"), ("DVD_END_DT", "19870101")]
    con.bulkref("101 HK EQUITY", "DVD_HIST", ovrds=ovrds)


@ifbbg
def test_bulkref_empty_field(con):
    df = con.bulkref(["88428LAA0 Corp"], ["INDEX_LIST"])
    df_exp = pd.DataFrame(
        [["88428LAA0 Corp", "INDEX_LIST", np.NaN, np.NaN, np.NaN]],
        columns=["ticker", "field", "name", "value", "position"]
    )
    assert_frame_equal(df, df_exp)


@ifbbg
def test_bulkref_empty_with_nonempty_field_smoketest(con):
    con.bulkref(['88428LAA0 Corp'], ['INDEX_LIST', 'USE_OF_PROCEEDS'])


@ifbbg
def test_bulkref_not_applicable_field(con):
    df = con.bulkref("CL1 Comdty", ["FUT_DLVRBLE_BNDS_ISINS"])
    df_exp = pd.DataFrame(
        [["CL1 Comdty", "FUT_DLVRBLE_BNDS_ISINS", np.NaN, np.NaN, np.NaN]],
        columns=["ticker", "field", "name", "value", "position"]
    )
    assert_frame_equal(df, df_exp)


@ifbbg
def test_bulkref_not_applicable_with_applicable_field_smoketest(con):
    con.bulkref('CL1 Comdty', ['OPT_CHAIN', 'FUT_DLVRBLE_BNDS_ISINS'])


# REF_HIST TESTS
@ifbbg
def test_hist_ref_one_ticker_one_field_numeric(con):
    dates = ["20160104", "20160105"]
    df = con.ref_hist("AUD1M CMPN Curncy", "DAYS_TO_MTY", dates)
    df_expect = pd.DataFrame(
        {"date": dates,
         "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
         "field": ["DAYS_TO_MTY", "DAYS_TO_MTY"],
         "value": [33, 32]}
    )
    assert_frame_equal(df, df_expect)


@ifbbg
def test_hist_ref_one_ticker_one_field_non_numeric(con):
    dates = ["20160104", "20160105"]
    df = con.ref_hist("AUD1M CMPN Curncy", "SETTLE_DT", dates)
    df_expect = pd.DataFrame(
        {"date": dates,
         "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
         "field": ["SETTLE_DT", "SETTLE_DT"],
         "value": 2 * [pd.datetime(2016, 2, 8).date()]}
    )
    assert_frame_equal(df, df_expect)


# BULKREF_HIST TESTS
@ifbbg
def test_bulkref_hist_one_field(con, data_path):
    dates = ["20150530", "20160530"]
    df = con.bulkref_hist('BCOM Index', 'INDX_MWEIGHT', dates=dates,
                          date_field='END_DATE_OVERRIDE')
    df_expected = pd.read_csv(
        os.path.join(data_path, "bulkref_20150530_20160530.csv")
    )
    pivot_and_assert(df, df_expected, with_date=True)


@ifbbg
def test_bulkhist_ref_with_alternative_reference_field(con):
    # smoke test to  check that the response was sent off and correctly
    # received
    dates = ["20160625"]
    con.bulkref_hist("BVIS0587 Index", "CURVE_TENOR_RATES", dates,
                     date_field="CURVE_DATE")


@ifbbg
def test_context_manager(port, host):
    with pdblp.bopen(host=host, port=port) as bb:
        df = bb.bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630')
    midx = pd.MultiIndex(levels=[["SPY US Equity"], ["PX_LAST"]],
                         labels=[[0], [0]], names=["ticker", "field"])
    df_expect = pd.DataFrame(
        index=pd.date_range("2015-06-29", "2015-06-30"),
        columns=midx,
        data=[205.42, 205.85]
    )
    df_expect.index.names = ["date"]
    assert_frame_equal(df, df_expect)


@ifbbg
def test_context_manager_passed_session(port, host):
    sopts = blpapi.SessionOptions()
    sopts.setServerHost(host)
    sopts.setServerPort(port)
    session = blpapi.Session(sopts)
    session.start()
    session.nextEvent(1000)
    session.nextEvent(1000)
    with pdblp.bopen(session=session) as bb:  # NOQA
        pass


@ifbbg
def test_multi_start(port, host, timeout):
    con = pdblp.BCon(host=host, port=port, timeout=timeout)
    con.start()
    con.start()


@ifbbg
def test_non_empty_session_queue(port, host):
    sopts = blpapi.SessionOptions()
    sopts.setServerHost(host)
    sopts.setServerPort(port)
    session = blpapi.Session(sopts)
    session.start()
    with pytest.raises(ValueError):
        pdblp.BCon(session=session)


@ifbbg
def test_bsrch(con):
    df = con.bsrch("COMDTY:VESSEL").head()
    df_expect = pd.DataFrame(["IMO1000019 Index", "LADY K II",
                              "IMO1000021 Index", "MONTKAJ",
                              "IMO1000033 Index"])
    assert_frame_equal(df, df_expect)


@ifbbg
def test_secf(con):
    df = con.secf(query="IBM",
                  yk_filter="EQTY",
                  max_results=1)
    df_expect = pd.DataFrame(
        {"ticker": ["IBM US EQUITY"],
         "description": ["International Business Machines Corp (U.S.)"]}
    )
    assert_frame_equal(df, df_expect)


def test_connection_error(port):
    con = pdblp.BCon(port=port+1)
    with pytest.raises(ConnectionError):
        con.start()
