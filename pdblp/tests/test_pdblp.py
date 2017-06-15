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
        df = self.con.bdib('SPY US Equity', '2017-06-12T10:00:00',
                           '2017-06-12T10:20:01', event_type="BID",
                           interval=10)
        idx = pd.DatetimeIndex(["2017-06-12T10:00:00", "2017-06-12T10:10:00",
                                "2017-06-12T10:20:00"])
        data = [[242.84, 242.84, 242.76, 242.84, 12535, 277],
                [242.84, 242.87, 242.76, 242.79, 7790, 194],
                [242.79, 242.79, 242.76, 242.79, 615, 13]]
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
        assert isinstance(df, pd.DataFrame)

    def test_ref_two_ticker_one_field_many_output(self):
        df = self.con.ref(['CL1 Comdty', 'CO1 Comdty'], 'FUT_CHAIN')
        # unknown / changing data returned so just assert right type
        assert isinstance(df, pd.DataFrame)

    def test_ref_two_ticker_two_field_many_output(self):
        df = self.con.ref(['CL1 Comdty', 'CO1 Comdty'],
                          ['FUT_CHAIN', 'FUT_CUR_GEN_TICKER'])
        # unknown / changing data returned so just assert right type
        assert isinstance(df, pd.DataFrame)

    def test_ref_one_ticker_one_field_override(self):
        df = self.con.ref('AUD Curncy', 'SETTLE_DT',
                          [("REFERENCE_DATE", "20161010")])
        df_expect = pd.DataFrame(
            columns=["ticker", "field", "value"],
            data=[["AUD Curncy", "SETTLE_DT",
                  pd.datetime(2016, 10, 12).date()]]
        )
        assert_frame_equal(df, df_expect)

    def test_ref_invalid_field(self):

        def run_query():
            self.con.ref(["EI862261 Corp"], ["not_a_field"])

        self.assertRaises(ValueError, run_query)

    def test_ref_not_applicable_field(self):
        df = self.con.ref(["EI862261 Corp"], ["MATURITY"])
        df_expect = pd.DataFrame([["EI862261 Corp", "MATURITY", pd.np.NaN]],
                                 columns=['ticker', 'field', 'value'])
        assert_frame_equal(df, df_expect)

    def test_ref_invalid_security(self):

        def run_query():
            self.con.ref(["NOT_A_TICKER"], ["MATURITY"])

        self.assertRaises(ValueError, run_query)

    def test_ref_applicable_with_not_applicable_field(self):
        df = self.con.ref("BVIS0587 Index", ["MATURITY", "NAME"])
        df_exp = pd.DataFrame(
            [["BVIS0587 Index", "MATURITY", pd.np.NaN],
             ["BVIS0587 Index", "NAME", "CAD Canada Govt BVAL Curve"]],
            columns=["ticker", "field", "value"])
        assert_frame_equal(df, df_exp)

    def test_ref_nested_array_field_data(self):
        # check only that "field" is a concatenation of top and nested
        # field values
        df = self.con.ref("BVIS0587 Index", ["CURVE_TENOR_RATES"])
        p1 = "CURVE_TENOR_RATES:"
        self.assertTrue((df.field == p1 + "Tenor").any())
        self.assertTrue((df.field == p1 + "Tenor Ticker").any())
        self.assertTrue((df.field == p1 + "Ask Yield").any())
        self.assertTrue((df.field == p1 + "Mid Yield").any())
        self.assertTrue((df.field == p1 + "Bid Yield").any())
        self.assertTrue((df.field == p1 + "Last Update").any())

    def test_hist_ref_one_ticker_one_field_numeric(self):
        dates = ["20160104", "20160105"]
        df = self.con.ref_hist("AUD1M CMPN Curncy", "DAYS_TO_MTY", dates)
        df_expect = pd.DataFrame(
            {"date": dates,
             "field": ["DAYS_TO_MTY", "DAYS_TO_MTY"],
             "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
             "value": [33, 32]}
        )
        assert_frame_equal(df, df_expect)

    def test_hist_ref_one_ticker_one_field_non_numeric(self):
        dates = ["20160104", "20160105"]
        df = self.con.ref_hist("AUD1M CMPN Curncy", "SETTLE_DT", dates)
        df_expect = pd.DataFrame(
            {"date": dates,
             "field": ["SETTLE_DT", "SETTLE_DT"],
             "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
             "value": 2 * [pd.datetime(2016, 2, 8).date()]}
        )
        assert_frame_equal(df, df_expect)

    def test_hist_ref_with_alternative_reference_field(self):
        dates = ["20160625"]
        df = self.con.ref_hist("BVIS0587 Index", "CURVE_TENOR_RATES", dates,
                               date_field="CURVE_DATE")
        # simply check that the response was sent off and correctly received
        self.assertIsInstance(df, pd.DataFrame)

    def test_context_manager(self):
        with pdblp.bopen(port=IP_PORT) as bb:
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

    def test_connection_error(self):
        con = pdblp.BCon(port=1111)

        def try_con():
            con.start()

        self.assertRaises(ConnectionError, try_con)

    def test_bsrch(self):
        df = self.con.bsrch("COMDTY:VESSEL").head()
        df_expect = pd.DataFrame(["IMO1000019 Index", "LADY K II",
                                  "IMO1000021 Index", "MONTKAJ",
                                  "IMO1000033 Index"])
        assert_frame_equal(df, df_expect)
