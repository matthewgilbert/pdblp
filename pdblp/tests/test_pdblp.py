import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal
from pandas.util.testing import assertIsInstance
from pdblp import pdblp

IP_PORT = 8194


class TestBCon(unittest.TestCase):

    def setUp(self):
        self.con = pdblp.BCon(port=IP_PORT)
        self.con.start()

    def tearDown(self):
        pass

    def test_bdh_one_ticker_one_field_pivoted(self):
        df = self.con.bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630')
        midx = pd.MultiIndex(levels=[["SPY US Equity"], ["PX_LAST"]],
                             labels=[[0], [0]], names=["ticker", "field"])
        df_expect = pd.DataFrame(
            index=pd.date_range("2015-06-29", "2015-06-30"),
            columns=midx,
            data=[205.42, 205.85]
        )
        df_expect.index.names = ["date"]
        assert_frame_equal(df, df_expect)

    def test_bdh_one_ticker_one_field_longdata(self):
        df = self.con.bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630',
                          longdata=True)
        idx = pd.Index(["date", "ticker", "field", "value"])
        data = [["2015-06-29", "2015-06-30"],
                ["SPY US Equity", "SPY US Equity"], ["PX_LAST", "PX_LAST"],
                [205.42, 205.85]]
        df_expect = pd.DataFrame(data=data, index=idx).transpose()
        df_expect.loc[:, "date"] = pd.to_datetime(df_expect.loc[:, "date"])
        df_expect.loc[:, "value"] = pd.np.float64(df_expect.loc[:, "value"])
        assert_frame_equal(df, df_expect)

    def test_bdh_one_ticker_two_field_pivoted(self):
        cols = ['PX_LAST', 'VOLUME']
        df = self.con.bdh('SPY US Equity', cols, '20150629', '20150630')
        midx = pd.MultiIndex(
            levels=[["SPY US Equity"], cols],
            labels=[[0, 0], [0, 1]], names=["ticker", "field"]
        )
        df_expect = pd.DataFrame(
            index=pd.date_range("2015-06-29", "2015-06-30"),
            columns=midx,
            data=[[205.42, 202621332], [205.85, 182925106]]
        )
        df_expect = df_expect.astype(pd.np.float64)
        df_expect.index.names = ["date"]
        assert_frame_equal(df, df_expect)

    def test_bdh_one_ticker_two_field_longdata(self):
        cols = ['PX_LAST', 'VOLUME']
        df = self.con.bdh('SPY US Equity', cols, '20150629', '20150630',
                          longdata=True)
        idx = pd.Index(["date", "ticker", "field", "value"])
        data = [["2015-06-29", "2015-06-29", "2015-06-30", "2015-06-30"],
                ["SPY US Equity", "SPY US Equity", "SPY US Equity", "SPY US Equity"],  # NOQA
                ["PX_LAST", "VOLUME", "PX_LAST", "VOLUME"],
                [205.42, 202621332, 205.85, 182925106]]
        df_expect = pd.DataFrame(data=data, index=idx).transpose()
        df_expect.loc[:, "date"] = pd.to_datetime(df_expect.loc[:, "date"])
        df_expect.loc[:, "value"] = pd.np.float64(df_expect.loc[:, "value"])
        assert_frame_equal(df, df_expect)

    def test_bdib(self):
        df = self.con.bdib('SPY US Equity', '2016-10-17T10:00:00',
                           '2016-10-17T10:20:01', event_type="BID",
                           interval=10)
        idx = pd.DatetimeIndex(["2016-10-17T10:00:00", "2016-10-17T10:10:00",
                                "2016-10-17T10:20:00"])
        data = [[212.36, 212.40, 212.24, 212.26, 1576, 76],
                [212.26, 212.36, 212.23, 212.23, 4432, 180],
                [212.24, 212.26, 212.23, 212.24, 480, 24]]
        cols = ["open", "high", "low", "close", "volume", "numEvents"]
        df_expect = pd.DataFrame(data=data, index=idx, columns=cols)
        assert_frame_equal(df, df_expect)

    def test_ref_one_ticker_one_field(self):
        df = self.con.ref('AUD Curncy', 'NAME')
        df_expect = pd.DataFrame(
            columns=["ticker", "field", "value"],
            data=[["AUD Curncy", "NAME", "Australian Dollar Spot"]]
        )
        assert_frame_equal(df, df_expect)

    def test_ref_one_ticker_one_field_many_output(self):
        df = self.con.ref('CL1 Comdty', 'FUT_CHAIN')
        # unknown / changing data returned so just assert right type
        assertIsInstance(df, pd.DataFrame)

    def test_ref_two_ticker_one_field_many_output(self):
        df = self.con.ref(['CL1 Comdty', 'CO1 Comdty'], 'FUT_CHAIN')
        # unknown / changing data returned so just assert right type
        assertIsInstance(df, pd.DataFrame)

    def test_ref_two_ticker_two_field_many_output(self):
        df = self.con.ref(['CL1 Comdty', 'CO1 Comdty'],
                          ['FUT_CHAIN', 'FUT_CUR_GEN_TICKER'])
        # unknown / changing data returned so just assert right type
        assertIsInstance(df, pd.DataFrame)

    def test_ref_one_ticker_one_field_override(self):
        df = self.con.ref('AUD Curncy', 'SETTLE_DT',
                          [("REFERENCE_DATE", "20161010")])
        df_expect = pd.DataFrame(
            columns=["ticker", "field", "value"],
            data=[["AUD Curncy", "SETTLE_DT",
                  pd.datetime(2016, 10, 12).date()]]
        )
        assert_frame_equal(df, df_expect)

    def test_hist_ref_one_ticker_one_field_longdata_numeric(self):
        df = self.con.ref_hist("AUD1M CMPN Curncy", "DAYS_TO_MTY", "20160104", "20160105", longdata=True)  # NOQA
        df_expect = pd.DataFrame(
            {"date": pd.date_range("2016-01-04", "2016-01-05"),
             "field": ["DAYS_TO_MTY", "DAYS_TO_MTY"],
             "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
             "value": [33, 32]}
        )
        assert_frame_equal(df, df_expect)

    def test_hist_ref_one_ticker_one_field_pivoted_numeric(self):
        df = self.con.ref_hist("AUD1M CMPN Curncy", "DAYS_TO_MTY", "20160104", "20160105")  # NOQA
        midx = pd.MultiIndex(
            levels=[["AUD1M CMPN Curncy"], ["DAYS_TO_MTY"]],
            labels=[[0], [0]], names=["ticker", "field"]
        )
        df_expect = pd.DataFrame(
            index=pd.date_range("2016-01-04", "2016-01-05"),
            columns=midx,
            data=[33, 32]
        )
        df_expect.index.names = ["date"]
        assert_frame_equal(df, df_expect)

    def test_hist_ref_one_ticker_one_field_longdata_non_numeric(self):
        df = self.con.ref_hist("AUD1M CMPN Curncy", "SETTLE_DT", "20160104", "20160105", longdata=True)  # NOQA
        df_expect = pd.DataFrame(
            {"date": pd.date_range("2016-01-04", "2016-01-05"),
             "field": ["SETTLE_DT", "SETTLE_DT"],
             "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
             "value": 2 * [pd.datetime(2016, 2, 8).date()]}
        )
        assert_frame_equal(df, df_expect)

    def test_hist_ref_one_ticker_one_field_pivoted_non_numeric(self):
        df = self.con.ref_hist("AUD1M CMPN Curncy", "SETTLE_DT", "20160104", "20160105")  # NOQA
        midx = pd.MultiIndex(
            levels=[["AUD1M CMPN Curncy"], ["SETTLE_DT"]],
            labels=[[0], [0]], names=["ticker", "field"]
        )
        df_expect = pd.DataFrame(
            index=pd.date_range("2016-01-04", "2016-01-05"),
            columns=midx,
            data=2 * [pd.datetime(2016, 2, 8).date()]
        )
        df_expect.index.names = ["date"]
        assert_frame_equal(df, df_expect)
