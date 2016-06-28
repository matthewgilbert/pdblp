import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal
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
                             labels=[[0],[0]], names=["ticker", "field"])
        df_expected = pd.DataFrame(
                        index=pd.date_range("2015-06-29", "2015-06-30"),
                        columns=midx,
                        data=[205.42, 205.85]
                      )
        df_expected.index.names = ["date"]
        assert_frame_equal(df, df_expected)

    def test_bdh_one_ticker_one_field_longdata(self):
        df = self.con.bdh('SPY US Equity', 'PX_LAST', '20150629', '20150630',
                          longdata=True)
        idx = pd.Index(["date", "ticker", "field", "value"])
        data = [["2015-06-29", "2015-06-30"],
                ["SPY US Equity", "SPY US Equity"], ["PX_LAST", "PX_LAST"],
                [205.42, 205.85]]
        df_expected = pd.DataFrame(data=data, index=idx).transpose()
        df_expected.loc[:, "date"] = pd.to_datetime(df_expected.loc[:, "date"])
        df_expected.loc[:, "value"] = pd.np.float64(df_expected.loc[:, "value"])
        assert_frame_equal(df, df_expected)

    def test_bdh_one_ticker_two_field_pivoted(self):
        cols = ['PX_LAST', 'VOLUME']
        df = self.con.bdh('SPY US Equity', cols, '20150629', '20150630')
        midx = pd.MultiIndex(levels=[["SPY US Equity"], cols],
                             labels=[[0, 0],[0, 1]], names=["ticker", "field"])
        df_expected = pd.DataFrame(
                        index=pd.date_range("2015-06-29", "2015-06-30"),
                        columns=midx,
                        data=[[205.42, 202621332], [205.85, 182925106]]
                      )
        df_expected = df_expected.astype(pd.np.float64)
        df_expected.index.names = ["date"]
        assert_frame_equal(df, df_expected)

    def test_bdh_one_ticker_two_field_longdata(self):
        cols = ['PX_LAST', 'VOLUME']
        df = self.con.bdh('SPY US Equity', cols, '20150629', '20150630',
                          longdata=True)
        midx = pd.MultiIndex(levels=[["SPY US Equity"], cols],
                             labels=[[0, 0],[0, 1]], names=["ticker", "field"])
        idx = pd.Index(["date", "ticker", "field", "value"])
        data = [["2015-06-29", "2015-06-29", "2015-06-30", "2015-06-30"],
                ["SPY US Equity", "SPY US Equity", "SPY US Equity", "SPY US Equity"],  # NOQA
                ["PX_LAST", "VOLUME", "PX_LAST", "VOLUME"],
                [205.42, 202621332, 205.85, 182925106]]
        df_expected = pd.DataFrame(data=data, index=idx).transpose()
        df_expected.loc[:, "date"] = pd.to_datetime(df_expected.loc[:, "date"])
        df_expected.loc[:, "value"] = pd.np.float64(df_expected.loc[:, "value"])
        assert_frame_equal(df, df_expected)

    def test_ref_one_ticker_one_field(self):
        df = self.con.ref('AUD Curncy', 'NAME')
        df_expected = pd.DataFrame(index=["NAME"], columns=["AUD Curncy"],
                                   data="Australian Dollar Spot")
        assert_frame_equal(df, df_expected)

    

