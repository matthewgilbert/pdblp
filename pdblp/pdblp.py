import blpapi
import logging
import datetime
import pandas as pd
from collections import defaultdict
from pandas import DataFrame


class BCon(object):
    def __init__(self, host='localhost', port=8194, debug=False):
        """
        Create an object which manages connection to the Bloomberg API session

        Parameters
        ----------
        host: str
            Host name
        port: int
            Port to connect to
        debug: Boolean {True, False}
            Boolean corresponding to whether to log Bloomberg Open API request
            and response messages to stdout
        """
        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)
        self._sessionOptions = sessionOptions
        # Create a Session
        self.session = blpapi.Session(sessionOptions)
        # initialize logger
        self.debug = debug

    @property
    def debug(self):
        """
        When True, print all Bloomberg Open API request and response messages
        to stdout
        """
        return self._debug

    @debug.setter
    def debug(self, value):
        """
        Set whether logging is True or False
        """
        self._debug = value
        root = logging.getLogger()
        if self._debug:
            # log requests and responses
            root.setLevel(logging.DEBUG)
        else:
            # log only failed connections
            root.setLevel(logging.INFO)

    def start(self):
        """
        start connection and init service for refData
        """
        # Start a Session
        if not self.session.start():
            logging.info("Failed to start session.")
            return
        self.session.nextEvent()
        # Open service to get historical data from
        if not self.session.openService("//blp/refdata"):
            logging.info("Failed to open //blp/refdata")
            return
        self.session.nextEvent()
        # Obtain previously opened service
        self.refDataService = self.session.getService("//blp/refdata")
        self.session.nextEvent()

    def restart(self):
        """
        Restart the blp session
        """
        # Recreate a Session
        self.session = blpapi.Session(self._sessionOptions)
        self.start()

    def _create_req(self, rtype, tickers, flds, ovrds, setvals):
        # flush event queue in case previous call errored out
        while(self.session.tryNextEvent()):
            pass

        request = self.refDataService.createRequest(rtype)
        for t in tickers:
            request.getElement("securities").appendValue(t)
        for f in flds:
            request.getElement("fields").appendValue(f)
        for name, val in setvals:
            request.set(name, val)

        overrides = request.getElement("overrides")
        for ovrd_fld, ovrd_val in ovrds:
            ovrd = overrides.appendElement()
            ovrd.setElement("fieldId", ovrd_fld)
            ovrd.setElement("value", ovrd_val)

        return request

    def bdh(self, tickers, flds, start_date, end_date, elms=[],
            ovrds=[], longdata=False):
        """
        Get tickers and fields, return pandas dataframe with column MultiIndex
        of tickers and fields if multiple fields given an Index otherwise.
        If single field is given DataFrame is ordered same as tickers,
        otherwise MultiIndex is sorted

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        start_date: string
            String in format YYYYmmdd
        end_date: string
            String in format YYYYmmdd
        elms: list of tuples
            List of tuples where each tuple corresponds to the other elements
            to be set, refer to A.2.4 HistoricalDataRequest in the
            Developers Guide for more info on these values
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value
        longdata: boolean
            Whether data should be returned in long data format or pivoted
        """

        data = self._bdh_list(tickers, flds, start_date, end_date,
                              elms, ovrds)

        df = DataFrame(data)
        df.columns = ["date", "ticker", "field", "value"]
        df.loc[:, "date"] = pd.to_datetime(df.loc[:, "date"])
        if not longdata:
            cols = ['ticker', 'field']
            df = df.set_index(['date'] + cols).unstack(cols)
            df.columns = df.columns.droplevel(0)

        return df

    def _bdh_list(self, tickers, flds, start_date, end_date, elms,
                  ovrds):

        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        setvals = elms
        setvals.append(("startDate", start_date))
        setvals.append(("endDate", end_date))

        request = self._create_req("HistoricalDataRequest", tickers, flds,
                                   ovrds, setvals)

        logging.debug("Sending Request:\n %s" % request)
        # Send the request
        self.session.sendRequest(request)
        data = []
        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                logging.debug("Message Received:\n %s" % msg)
                if msg.getElement('securityData').hasElement('securityError') or (msg.getElement('securityData').getElement("fieldExceptions").numValues() > 0):  # NOQA
                    raise Exception(msg)
                ticker = msg.getElement('securityData').getElement('security').getValue()  # NOQA
                fldDatas = msg.getElement('securityData').getElement('fieldData')  # NOQA
                for fd in fldDatas.values():
                    dt = fd.getElement('date').getValue()
                    for element in fd.elements():
                        fname = str(element.name())
                        if fname == "date":
                            continue
                        val = element.getValue()
                        data.append((dt, ticker, fname, val))
            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completely received, so we could exit
                break

        return data

    def ref(self, tickers, flds, ovrds=[]):
        """
        Make a reference data request, get tickers and fields, return long
        pandas Dataframe with columns [ticker, field, value]

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value
        """

        data = self._ref(tickers, flds, ovrds)

        data = DataFrame(data)
        data.columns = ["ticker", "field", "value"]
        return data

    def _ref(self, tickers, flds, ovrds):

        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        request = self._create_req("ReferenceDataRequest", tickers, flds,
                                   ovrds, [])

        logging.debug("Sending Request:\n %s" % request)
        # Send the request
        self.session.sendRequest(request)
        data = []
        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                logging.debug("Message Received:\n %s" % msg)
                fldData = msg.getElement('securityData')
                for i in range(fldData.numValues()):
                    ticker = (fldData.getValue(i).getElement("security").getValue())  # NOQA
                    reqFldsData = (fldData.getValue(i).getElement('fieldData'))
                    for j in range(reqFldsData.numElements()):
                        fld = flds[j]
                        # this is for dealing with requests which return arrays
                        # of values for a single field
                        if reqFldsData.getElement(fld).isArray():
                            lrng = reqFldsData.getElement(fld).numValues()
                            for k in range(lrng):
                                elms = (reqFldsData.getElement(fld).getValue(k).elements())  # NOQA
                                # if the elements of the array have multiple
                                # subelements this will just append them all
                                # into a list
                                for elm in elms:
                                    data.append([ticker, fld, elm.getValue()])
                        else:
                            val = reqFldsData.getElement(fld).getValue()
                            data.append([ticker, fld, val])

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completely received, so we could exit
                break

        return data

    def ref_hist(self, tickers, flds, start_date,
                 end_date=datetime.date.today().strftime('%Y%m%d'),
                 timeout=2000, longdata=False):
        """
        Get tickers and fields, periodically override REFERENCE_DATE to create
        a time series. Return pandas dataframe with column MultiIndex
        of tickers and fields if multiple fields given, Index otherwise.
        If single field is given DataFrame is ordered same as tickers,
        otherwise MultiIndex is sorted

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        start_date: string
            String in format YYYYmmdd
        end_date: string
            String in format YYYYmmdd
        timeout: int
            Passed into nextEvent(timeout), number of milliseconds before
            timeout occurs
        """
        # correlationIDs should be unique to a session so rather than
        # managing unique IDs for the duration of the session just restart
        # a session for each call
        self.restart()
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]
        # Create and fill the request for the historical data
        request = self.refDataService.createRequest("ReferenceDataRequest")
        for t in tickers:
            request.getElement("securities").appendValue(t)
        for f in flds:
            request.getElement("fields").appendValue(f)

        overrides = request.getElement("overrides")
        dates = pd.date_range(start_date, end_date, freq='b')
        ovrd = overrides.appendElement()
        for dt in dates:
            ovrd.setElement("fieldId", "REFERENCE_DATE")
            ovrd.setElement("value", dt.strftime('%Y%m%d'))
            # CorrelationID used to keep track of which response coincides with
            # which request
            cid = blpapi.CorrelationId(dt)
            logging.debug("Sending Request:\n %s" % request)
            self.session.sendRequest(request, correlationId=cid)
        data = []
        # Process received events
        while(True):
            ev = self.session.nextEvent(timeout)
            for msg in ev:
                logging.debug("Message Received:\n %s" % msg)
                corrID = msg.correlationIds()[0].value()
                fldData = msg.getElement('securityData')
                for i in range(fldData.numValues()):
                    tckr = (fldData.getValue(i).getElement("security").getValue())  # NOQA
                    reqFldsData = (fldData.getValue(i).getElement('fieldData'))
                    for j in range(reqFldsData.numElements()):
                        fld = flds[j]
                        val = reqFldsData.getElement(fld).getValue()
                        data.append((fld, tckr, val, corrID))
            if ev.eventType() == blpapi.Event.TIMEOUT:
                # All events processed
                if (len(data) / len(flds) / len(tickers)) == len(dates):
                    break
                else:
                    raise(RuntimeError("Timeout, increase timeout parameter"))
        data = pd.DataFrame(data)
        data.columns = ['field', 'ticker', 'value', 'date']
        data = data.sort_values(by='date')
        data = data.reset_index(drop=True)
        data = data.loc[:, ['date', 'field', 'ticker', 'value']]

        if not longdata:
            cols = ['ticker', 'field']
            data = data.set_index(['date'] + cols).unstack(cols)
            data.columns = data.columns.droplevel(0)

        return data

    def bdib(self, ticker, start_datetime, end_datetime, event_type, interval,
             elms=[]):
        """
        Get Open, High, Low, Close, Volume, and numEvents for a ticker.
        Return pandas dataframe

        Parameters
        ----------
        ticker: string
            String corresponding to ticker
        start_datetime: string
            UTC datetime in format YYYY-mm-ddTHH:MM:SS
        end_datetime: string
            UTC datetime in format YYYY-mm-ddTHH:MM:SS
        event_type: string {TRADE, BID, ASK, BID_BEST, ASK_BEST, BEST_BID,
                           BEST_ASK}
            Requested data event type
        interval: int {1... 1440}
            Length of time bars
        elms: list of tuples
            List of tuples where each tuple corresponds to the other elements
            to be set, refer to A.2.8 IntradayBarRequest in the
            Developers Guide for more info on these values
        """
        # flush event queue in case previous call errored out
        while(self.session.tryNextEvent()):
            pass

        # Create and fill the request for the historical data
        request = self.refDataService.createRequest("IntradayBarRequest")
        request.set("security", ticker)
        request.set("eventType", event_type)
        request.set("interval", interval)  # bar interval in minutes
        request.set("startDateTime", start_datetime)
        request.set("endDateTime", end_datetime)
        for name, val in elms:
            request.set(name, val)

        logging.debug("Sending Request:\n %s" % request)
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        # Process received events
        flds = ['open', 'high', 'low', 'close', 'volume', 'numEvents']
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                logging.debug("Message Received:\n %s" % msg)
                barTick = (msg.getElement('barData')
                           .getElement('barTickData'))
                for i in range(barTick.numValues()):
                    for fld in flds:
                        dt = barTick.getValue(i).getElement(0).getValue()
                        val = (barTick.getValue(i).getElement(fld).getValue())
                        data[(fld)][dt] = val

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
        data = DataFrame(data)
        if not data.empty:
            data.index = pd.to_datetime(data.index)
            data = data[flds]
        return data

    def stop(self):
        """
        Close the blp session
        """
        self.session.stop()
