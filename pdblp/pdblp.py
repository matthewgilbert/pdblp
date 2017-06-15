import blpapi
import logging
import pandas as pd
import contextlib
from collections import defaultdict
from pandas import DataFrame


@contextlib.contextmanager
def bopen(**kwargs):
    """
    Open and manage a BCon wrapper to a Bloomberg API session

    Parameters
    ----------
    **kwargs:
        Keyword arguments passed into pdblp.BCon initialization
    """
    con = BCon(**kwargs)
    con.start()
    try:
        yield con
    finally:
        con.stop()


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
            raise ConnectionError("Could not start a blpapi.session")
        self.session.nextEvent()
        # Open service to get historical data from
        if not self.session.openService("//blp/refdata"):
            logging.info("Failed to open //blp/refdata")
            raise ConnectionError("Could not open a //blp/refdata service")
        self.session.nextEvent()
        # Obtain previously opened service
        self.refDataService = self.session.getService("//blp/refdata")
        self.session.nextEvent()
        # Open BSearch Service
        if not self.session.openService("//blp/exrsvc"):
            logging.error("Failed to open //blp/exrsvc")
            raise ConnectionError("Could not open a //blp/exrsvc service")
        # Obtain previously opened service
        self.exrService = self.session.getService("//blp/exrsvc")
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
            to be set, e.g. [("periodicityAdjustment", "ACTUAL")]
            Refer to A.2.4 HistoricalDataRequest in the Developers Guide for
            more info on these values
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value
        longdata: boolean
            Whether data should be returned in long data format or pivoted
        """

        elms = list(elms)

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

        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        request = self._create_req("ReferenceDataRequest", tickers, flds,
                                   ovrds, [])

        logging.debug("Sending Request:\n %s" % request)
        self.session.sendRequest(request)
        data = self._parse_ref(flds)
        data = DataFrame(data)
        data.columns = ["ticker", "field", "value"]
        return data

    def _parse_ref(self, flds, keep_corrId=False, sent_events=1, timeout=500):
        data = []
        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(timeout)
            for msg in ev:
                logging.debug("Message Received:\n %s" % msg)
                if keep_corrId:
                    corrId = [msg.correlationIds()[0].value()]
                else:
                    corrId = []
                secDataArray = msg.getElement('securityData')
                for secDataElm in secDataArray.values():
                    ticker = secDataElm.getElement("security").getValue()
                    if secDataElm.hasElement("securityError"):
                        raise ValueError("Unknow security %s" % ticker)
                    self._check_fieldExceptions(secDataElm.getElement("fieldExceptions"))  # NOQA
                    fieldData = secDataElm.getElement('fieldData')
                    for j in range(len(flds)):
                        fld = flds[j]
                        # this is a slight hack but if a fieldData response
                        # does not have the element fld and this is not a bad
                        # field (which is checked above) then the assumption is
                        # that this is a not applicable field, thus set NaN
                        if not fieldData.hasElement(fld):
                            dataj = [ticker, fld, pd.np.NaN]
                            dataj.extend(corrId)
                            data.append(dataj)
                        # this is for dealing with requests which return arrays
                        # of values for a single field
                        elif fieldData.getElement(fld).isArray():
                            for field in fieldData.getElement(fld).values():
                                # if the elements of the array have multiple
                                # subelements this will just append them all
                                # into a list
                                for elm in field.elements():
                                    mfld = fld + ":" + str(elm.name())
                                    dataj = [ticker, mfld, elm.getValue()]
                                    dataj.extend(corrId)
                                    data.append(dataj)
                        else:
                            val = fieldData.getElement(fld).getValue()
                            dataj = [ticker, fld, val]
                            dataj.extend(corrId)
                            data.append(dataj)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completely received, so we could exit
                sent_events = sent_events - 1
                if sent_events == 0:
                    break
            # for ref_hist() calls this occassionally times out
            elif ev.eventType() == blpapi.Event.TIMEOUT:
                raise RuntimeError("Timeout, increase timeout parameter")

        return data

    @staticmethod
    def _check_fieldExceptions(fieldExceptions):
        # iterate over an array of fieldExceptions and check for a
        # INVALID_FIELD error

        INVALID_FIELD = 'INVALID_FIELD'
        for fe in fieldExceptions.values():
            category = fe.getElement("errorInfo").getElement("subcategory").getValue()  # NOQA
            if category == INVALID_FIELD:
                raise ValueError("%s: %s" % (fe.getElement("fieldId").getValue(), category))  # NOQA

    def ref_hist(self, tickers, flds, dates, timeout=2000, ovrds=[],
                 date_field="REFERENCE_DATE"):
        """
        Get tickers and fields, periodically override date_field to create
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
        dates: list
            list of date strings in the format YYYYmmdd
        timeout: int
            Passed into nextEvent(timeout), number of milliseconds before
            timeout occurs
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value. This should not include the date_field which will
            be iteratively overridden
        date_field: str
            Field to iteratively override for requesting historical data,
            e.g. REFERENCE_DATE, CURVE_DATE, etc.
        """
        # correlationIDs should be unique to a session so rather than
        # managing unique IDs for the duration of the session just restart
        # a session for each call
        self.restart()
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        request = self._create_req("ReferenceDataRequest", tickers, flds,
                                   ovrds, [])

        overrides = request.getElement("overrides")
        if len(dates) == 0:
            raise ValueError("dates must by non empty")
        ovrd = overrides.appendElement()
        for dt in dates:
            ovrd.setElement("fieldId", date_field)
            ovrd.setElement("value", dt)
            # CorrelationID used to keep track of which response coincides with
            # which request
            cid = blpapi.CorrelationId(dt)
            logging.debug("Sending Request:\n %s" % request)
            self.session.sendRequest(request, correlationId=cid)

        data = self._parse_ref(flds, keep_corrId=True, sent_events=len(dates),
                               timeout=timeout)
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'value', 'date']
        data = data.sort_values(by='date')
        data = data.reset_index(drop=True)
        data = data.loc[:, ['date', 'field', 'ticker', 'value']]
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

    def bsrch(self, domain):
        """This function uses the Bloomberg API to retrieve 'bsrch'
        (Bloomberg SRCH Data) queries. Returns list of tickers.

        Parameters
        ----------
        domain: string
        A character string with the name of the domain to execute.
        It can be a user defined SRCH screen, commodity screen or
        one of the variety of Bloomberg examples. All domains are in the format
        <domain>:<search_name>. Example "COMDTY:NGFLOW"
        Returns
        -------
        data: pandas.DataFrame
        List of bloomberg tickers from the BSRCH
        """
        request = self.exrService.createRequest("ExcelGetGridRequest")
        request.set("Domain", domain)
        self.session.sendRequest(request)
        data = []
        # Process received events
        while True:
            # We provide timeout to give the chance for Ctrl+C handling:
            event = self.session.nextEvent(0)
            if event.eventType() == blpapi.Event.RESPONSE or \
               event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in event:
                    logging.debug(msg)
                    for v in msg.getElement("DataRecords").values():
                        for f in v.getElement("DataFields").values():
                            data.append(f.getElementAsString("StringValue"))
            if event.eventType() == blpapi.Event.RESPONSE:
                break
        return pd.DataFrame(data)

    def stop(self):
        """
        Close the blp session
        """
        self.session.stop()
