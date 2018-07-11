import unittest
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal
from pdblp import pdblp
import os


IP_PORT = 8194


class TestBCon(unittest.TestCase):

    def setUp(self):
        self.con = pdblp.BCon(port=IP_PORT, timeout=5000)
        self.con.start()
        cdir = os.path.dirname(__file__)
        self.path = os.path.join(cdir, 'data/')

    def tearDown(self):
        pass

    def pivot_and_assert(self, df, df_exp, with_date=False):
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

    def test_bdh_empty_data_only(self):
        df = self.con.bdh(
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

    def test_bdh_empty_data_with_non_empty_data(self):
        df = self.con.bdh(
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

    def test_bdh_partially_empty_data(self):
        df = self.con.bdh(
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
        df_expect.loc[:, "value"] = np.float64(df_expect.loc[:, "value"])
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
        df_expect = df_expect.astype(np.float64)
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
        df_expect.loc[:, "value"] = np.float64(df_expect.loc[:, "value"])
        assert_frame_equal(df, df_expect)

    def test_bdh_value_errors(self):
        bad_col = "not_a_fld"
        self.assertRaises(ValueError, self.con.bdh, "SPY US Equity", bad_col,
                          "20150630", "20150630")
        bad_ticker = "not_a_ticker"
        self.assertRaises(ValueError, self.con.bdh, bad_ticker, "PX_LAST",
                          "20150630", "20150630")

    def test_bdib(self):
        # BBG has limited history for the IntradayBarRequest service so this
        # needs to be periodically updated
        df = self.con.bdib('SPY US Equity', '2018-02-09T10:00:00',
                           '2018-02-09T10:20:01', event_type="BID",
                           interval=10)
        idx = pd.DatetimeIndex(["2018-02-09T10:00:00", "2018-02-09T10:10:00",
                                "2018-02-09T10:20:00"])
        data = [[260.85, 260.90, 260.50, 260.58, 8038, 938],
                [260.58, 260.72, 260.34, 260.64, 11795, 1460],
                [260.64, 260.78, 260.64, 260.77, 964, 116]]
        cols = ["open", "high", "low", "close", "volume", "numEvents"]
        df_expect = pd.DataFrame(data=data, index=idx, columns=cols)
        assert_frame_equal(df, df_expect)

    # REF TESTS

    def test_ref_one_ticker_one_field(self):
        df = self.con.ref('AUD Curncy', 'NAME')
        df_expect = pd.DataFrame(
            columns=["ticker", "field", "value"],
            data=[["AUD Curncy", "NAME", "Australian Dollar Spot"]]
        )
        assert_frame_equal(df, df_expect)

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
        self.assertRaises(ValueError, self.con.ref,
                          "EI862261 Corp", "not_a_field")

    def test_ref_not_applicable_field(self):
        # test both cases described in
        # https://github.com/matthewgilbert/pdblp/issues/6
        df = self.con.ref("BCOM Index", ["INDX_GWEIGHT"])
        df_expect = pd.DataFrame(
            [["BCOM Index", "INDX_GWEIGHT", np.NaN]],
            columns=['ticker', 'field', 'value']
        )
        assert_frame_equal(df, df_expect)

        df = self.con.ref("BCOM Index", ["INDX_MWEIGHT_PX2"])
        df_expect = pd.DataFrame(
            [["BCOM Index", "INDX_MWEIGHT_PX2", np.NaN]],
            columns=['ticker', 'field', 'value']
        )
        assert_frame_equal(df, df_expect)

    def test_ref_invalid_security(self):
        self.assertRaises(ValueError, self.con.ref, "NOT_A_TICKER", "MATURITY")

    def test_ref_applicable_with_not_applicable_field(self):
        df = self.con.ref("BVIS0587 Index", ["MATURITY", "NAME"])
        df_exp = pd.DataFrame(
            [["BVIS0587 Index", "MATURITY", np.NaN],
             ["BVIS0587 Index", "NAME", "CAD Canada Govt BVAL Curve"]],
            columns=["ticker", "field", "value"])
        assert_frame_equal(df, df_exp)

    def test_ref_mixed_data_error(self):
        # calling ref which returns singleton and array data throws error
        self.assertRaises(ValueError, self.con.ref, 'CL1 Comdty', 'FUT_CHAIN')

    # BULKREF TESTS

    def test_bulkref_one_ticker_one_field(self):
        df = self.con.bulkref('BCOM Index', 'INDX_MWEIGHT',
                              ovrds=[("END_DATE_OVERRIDE", "20150530")])
        df_expected = pd.read_csv(
            os.path.join(self.path, "bulkref_20150530.csv")
        )
        self.pivot_and_assert(df, df_expected)

    def test_bulkref_two_ticker_one_field(self):
        df = self.con.bulkref(['BCOM Index', 'OEX Index'], 'INDX_MWEIGHT',
                              ovrds=[("END_DATE_OVERRIDE", "20150530")])
        df_expected = pd.read_csv(
            os.path.join(self.path, "bulkref_two_fields_20150530.csv")
        )
        self.pivot_and_assert(df, df_expected)

    def test_bulkref_singleton_error(self):
        # calling bulkref which returns singleton throws error
        self.assertRaises(ValueError, self.con.bulkref, 'CL1 Comdty',
                          'FUT_CUR_GEN_TICKER')

    def test_bulkref_null_scalar_sub_element(self):
        # related to https://github.com/matthewgilbert/pdblp/issues/32#issuecomment-385555289  # NOQA
        # smoke test to check parse correctly
        ovrds = [("DVD_START_DT", "19860101"), ("DVD_END_DT", "19870101")]
        self.con.bulkref("101 HK EQUITY", "DVD_HIST", ovrds=ovrds)

    def test_bulkref_empty_field(self):
        df = self.con.bulkref(["88428LAA0 Corp"], ["INDEX_LIST"])
        df_exp = pd.DataFrame(
            [["88428LAA0 Corp", "INDEX_LIST", np.NaN, np.NaN, np.NaN]],
            columns=["ticker", "field", "name", "value", "position"]
        )
        assert_frame_equal(df, df_exp)

        # empty with non empty smoke test
        self.con.bulkref(['88428LAA0 Corp'], ['INDEX_LIST', 'USE_OF_PROCEEDS'])

    def test_bulkref_not_applicable_field(self):
        df = self.con.bulkref("CL1 Comdty", ["FUT_DLVRBLE_BNDS_ISINS"])
        df_exp = pd.DataFrame(
            [["CL1 Comdty", "FUT_DLVRBLE_BNDS_ISINS", np.NaN, np.NaN, np.NaN]],
            columns=["ticker", "field", "name", "value", "position"]
        )
        assert_frame_equal(df, df_exp)

        # with an applicable field smoke test
        self.con.bulkref('CL1 Comdty', ['OPT_CHAIN', 'FUT_DLVRBLE_BNDS_ISINS'])

    # REF_HIST TESTS

    def test_hist_ref_one_ticker_one_field_numeric(self):
        dates = ["20160104", "20160105"]
        df = self.con.ref_hist("AUD1M CMPN Curncy", "DAYS_TO_MTY", dates)
        df_expect = pd.DataFrame(
            {"date": dates,
             "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
             "field": ["DAYS_TO_MTY", "DAYS_TO_MTY"],
             "value": [33, 32]}
        )
        assert_frame_equal(df, df_expect)

    def test_hist_ref_one_ticker_one_field_non_numeric(self):
        dates = ["20160104", "20160105"]
        df = self.con.ref_hist("AUD1M CMPN Curncy", "SETTLE_DT", dates)
        df_expect = pd.DataFrame(
            {"date": dates,
             "ticker": ["AUD1M CMPN Curncy", "AUD1M CMPN Curncy"],
             "field": ["SETTLE_DT", "SETTLE_DT"],
             "value": 2 * [pd.datetime(2016, 2, 8).date()]}
        )
        assert_frame_equal(df, df_expect)

    # BULKREF_HIST TESTS

    def test_bulkref_hist_one_field(self):
        dates = ["20150530", "20160530"]
        df = self.con.bulkref_hist('BCOM Index', 'INDX_MWEIGHT', dates=dates,
                                   date_field='END_DATE_OVERRIDE')
        df_expected = pd.read_csv(
            os.path.join(self.path, "bulkref_20150530_20160530.csv")
        )
        self.pivot_and_assert(df, df_expected, with_date=True)

    def test_bulkhist_ref_with_alternative_reference_field(self):
        # smoke test to  check that the response was sent off and correctly
        # received
        dates = ["20160625"]
        self.con.bulkref_hist("BVIS0587 Index", "CURVE_TENOR_RATES", dates,
                              date_field="CURVE_DATE")

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
        self.assertRaises(ConnectionError, con.start)

    def test_bsrch(self):
        df = self.con.bsrch("COMDTY:VESSEL").head()
        df_expect = pd.DataFrame(["IMO1000019 Index", "LADY K II",
                                  "IMO1000021 Index", "MONTKAJ",
                                  "IMO1000033 Index"])
        assert_frame_equal(df, df_expect)
