import blpapi
import logging
import pandas as pd
import numpy as np
import contextlib
from collections import defaultdict


def _get_logger(debug):
    logger = logging.getLogger(__name__)
    if (logger.parent is not None) and logger.parent.hasHandlers():
        if debug:
            logger.warning("'pdblp.BCon.debug=True' is ignored when user "
                           "specifies logging event handlers")
    else:
        if not logger.handlers:
            formatter = logging.Formatter('%(name)s:%(levelname)s:%(message)s')
            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            logger.addHandler(sh)
        if debug:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.WARNING)

    return logger


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
    def __init__(self, host='localhost', port=8194, debug=False, timeout=500):
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
        timeout: int
            Number of milliseconds before timeout occurs when parsing response.
            See blp.Session.nextEvent() for more information.
        """
        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)
        self._sessionOptions = sessionOptions
        # Create a Session
        self.session = blpapi.Session(sessionOptions)
        self.timeout = timeout
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

    def start(self):
        """
        start connection and init service for refData
        """
        # Start a Session
        logger = _get_logger(self.debug)
        if not self.session.start():
            logger.warning("Failed to start session.")
            raise ConnectionError("Could not start a blpapi.session")
        self.session.nextEvent()
        # Open service to get historical data from
        if not self.session.openService("//blp/refdata"):
            logger.warning("Failed to open //blp/refdata")
            raise ConnectionError("Could not open a //blp/refdata service")
        self.session.nextEvent()
        # Obtain previously opened service
        self.refDataService = self.session.getService("//blp/refdata")
        self.session.nextEvent()
        # Open BSearch Service
        if not self.session.openService("//blp/exrsvc"):
            logger.warning("Failed to open //blp/exrsvc")
            raise ConnectionError("Could not open a //blp/exrsvc service")
        # Obtain previously opened service
        self.exrService = self.session.getService("//blp/exrsvc")
        self.session.nextEvent()
        return self

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

    def bdh(self, tickers, flds, start_date, end_date, elms=None,
            ovrds=None, longdata=False):
        """
        Get tickers and fields, return pandas DataFrame with columns as
        MultiIndex with levels "ticker" and "field" and indexed by "date".
        If long data is requested return DataFrame with columns
        ["date", "ticker", "field", "value"].

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
            to be set, e.g. [("periodicityAdjustment", "ACTUAL")].
            Refer to the HistoricalDataRequest section in the
            'Services & schemas reference guide' for more info on these values
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value
        longdata: boolean
            Whether data should be returned in long data format or pivoted
        """
        ovrds = [] if not ovrds else ovrds
        elms = [] if not elms else elms

        elms = list(elms)

        data = self._bdh_list(tickers, flds, start_date, end_date,
                              elms, ovrds)

        df = pd.DataFrame(data, columns=["date", "ticker", "field", "value"])
        df.loc[:, "date"] = pd.to_datetime(df.loc[:, "date"])
        if not longdata:
            cols = ['ticker', 'field']
            df = df.set_index(['date'] + cols).unstack(cols)
            df.columns = df.columns.droplevel(0)

        return df

    def _bdh_list(self, tickers, flds, start_date, end_date, elms,
                  ovrds):
        logger = _get_logger(self.debug)
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        setvals = elms
        setvals.append(("startDate", start_date))
        setvals.append(("endDate", end_date))

        request = self._create_req("HistoricalDataRequest", tickers, flds,
                                   ovrds, setvals)
        logger.info("Sending Request:\n %s" % request)
        # Send the request
        self.session.sendRequest(request)
        data = []
        # Process received events
        while(True):
            ev = self.session.nextEvent(self.timeout)
            for msg in ev:
                logger.info("Message Received:\n %s" % msg)
                has_security_error = (msg.getElement('securityData')
                                      .hasElement('securityError'))
                has_field_exception = (msg.getElement('securityData')
                                       .getElement("fieldExceptions")
                                       .numValues() > 0)
                if has_security_error or has_field_exception:
                    raise ValueError(msg)
                ticker = (msg.getElement('securityData')
                          .getElement('security').getValue())
                fldDatas = (msg.getElement('securityData')
                            .getElement('fieldData'))
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

    def ref(self, tickers, flds, ovrds=None):
        """
        Make a reference data request, get tickers and fields, return long
        pandas DataFrame with columns [ticker, field, value]

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> con.ref("CL1 Comdty", ["FUT_GEN_MONTH"])

        Notes
        -----
        This returns reference data which has singleton values. In raw format
        the messages passed back contain data of the form

        fieldData = {
                FUT_GEN_MONTH = "FGHJKMNQUVXZ"
        }
        """
        ovrds = [] if not ovrds else ovrds

        logger = _get_logger(self.debug)
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]
        request = self._create_req("ReferenceDataRequest", tickers, flds,
                                   ovrds, [])
        logger.info("Sending Request:\n %s" % request)
        self.session.sendRequest(request)
        data = self._parse_ref(flds)
        data = pd.DataFrame(data)
        data.columns = ["ticker", "field", "value"]
        return data

    def _parse_ref(self, flds, keep_corrId=False, sent_events=1):
        logger = _get_logger(self.debug)
        data = []
        # Process received events
        while(True):
            ev = self.session.nextEvent(self.timeout)
            for msg in ev:
                logger.info("Message Received:\n %s" % msg)
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
                        # avoid returning nested bbg objects, fail instead
                        # since user should use bulkref()
                        if fieldData.hasElement(fld):
                            if fieldData.getElement(fld).isArray():
                                raise ValueError("Field '{0}' returns bulk "
                                                 "reference data which is not "
                                                 "supported".format(fld))
                        # this is a slight hack but if a fieldData response
                        # does not have the element fld and this is not a bad
                        # field (which is checked above) then the assumption is
                        # that this is a not applicable field, thus set NaN
                        # see https://github.com/matthewgilbert/pdblp/issues/13
                        if not fieldData.hasElement(fld):
                            dataj = [ticker, fld, np.NaN]
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
            # calls can occassionally timeout in unpredictable manner due
            # to variability in BBG response times
            elif ev.eventType() == blpapi.Event.TIMEOUT:
                raise RuntimeError("Timeout, increase BCon.timeout attribute")
        return data

    def bulkref(self, tickers, flds, ovrds=None):
        """
        Make a bulk reference data request, get tickers and fields, return long
        pandas DataFrame with columns [ticker, field, name, value, position].
        Name refers to the element name and position is the position in the
        corresponding array returned.

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> con.bulkref('BCOM Index', 'INDX_MWEIGHT')

        Notes
        -----
        This returns bulk reference data which has array values. In raw format
        the messages passed back contain data of the form

        fieldData = {
            INDX_MWEIGHT[] = {
                INDX_MWEIGHT = {
                    Member Ticker and Exchange Code = "BON8"
                    Percentage Weight = 2.410000
                }
                INDX_MWEIGHT = {
                    Member Ticker and Exchange Code = "C N8"
                    Percentage Weight = 6.560000
                }
                INDX_MWEIGHT = {
                    Member Ticker and Exchange Code = "CLN8"
                    Percentage Weight = 7.620000
                }
            }
        }
        """
        ovrds = [] if not ovrds else ovrds

        logger = _get_logger(self.debug)
        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]
        setvals = []
        request = self._create_req("ReferenceDataRequest", tickers, flds,
                                   ovrds, setvals)
        logger.info("Sending Request:\n %s" % request)
        self.session.sendRequest(request)
        data = self._parse_bulkref(flds)
        data = pd.DataFrame(data)
        data.columns = ["ticker", "field", "name", "value", "position"]
        return data

    def _parse_bulkref(self, flds, keep_corrId=False, sent_events=1):
        logger = _get_logger(self.debug)
        data = []
        # Process received events
        while(True):
            ev = self.session.nextEvent(self.timeout)
            for msg in ev:
                logger.info("Message Received:\n %s" % msg)
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
                        if fieldData.hasElement(fld):
                            # fail coherently instead of parsing downstream
                            if not fieldData.getElement(fld).isArray():
                                raise ValueError("Cannot parse field '{0}' "
                                                 "which is not bulk reference "
                                                 "data".format(fld))
                            arrayValues = fieldData.getElement(fld).values()
                            for i, field in enumerate(arrayValues):
                                for elm in field.elements():
                                    value_name = str(elm.name())
                                    if not elm.isNull():
                                        val = elm.getValue()
                                    else:
                                        val = np.NaN
                                    dataj = [ticker, fld, value_name, val, i]
                                    dataj.extend(corrId)
                                    data.append(dataj)
                        else:  # field is empty or NOT_APPLICABLE_TO_REF_DATA
                            dataj = [ticker, fld, np.NaN, np.NaN, np.NaN]
                            dataj.extend(corrId)
                            data.append(dataj)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completely received, so we could exit
                sent_events = sent_events - 1
                if sent_events == 0:
                    break
            # calls can occassionally timeout in unpredictable manner due
            # to variability in BBG response times
            elif ev.eventType() == blpapi.Event.TIMEOUT:
                raise RuntimeError("Timeout, increase BCon.timeout attribute")
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

    def ref_hist(self, tickers, flds, dates, ovrds=None,
                 date_field="REFERENCE_DATE"):
        """
        Make iterative calls to ref() and create a long DataFrame with columns
        [date, ticker, field, value] where each date corresponds to overriding
        a historical data override field.

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        dates: list
            list of date strings in the format YYYYmmdd
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value. This should not include the date_field which will
            be iteratively overridden
        date_field: str
            Field to iteratively override for requesting historical data,
            e.g. REFERENCE_DATE, CURVE_DATE, etc.

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> dates = ["20160625", "20160626"]
        >>> con.ref_hist("AUD1M CMPN Curncy", "SETTLE_DT", dates)

        """
        # correlationIDs should be unique to a session so rather than
        # managing unique IDs for the duration of the session just restart
        # a session for each call

        ovrds = [] if not ovrds else ovrds

        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]

        self._send_hist(tickers, flds, dates, date_field, ovrds)

        data = self._parse_ref(flds, keep_corrId=True, sent_events=len(dates))
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'value', 'date']
        data = data.sort_values(by='date').reset_index(drop=True)
        data = data.loc[:, ['date', 'ticker', 'field', 'value']]
        return data

    def bulkref_hist(self, tickers, flds, dates, ovrds=None,
                     date_field="REFERENCE_DATE"):
        """
        Make iterative calls to bulkref() and create a long DataFrame with
        columns [date, ticker, field, name, value, position] where each date
        corresponds to overriding a historical data override field.

        Parameters
        ----------
        tickers: {list, string}
            String or list of strings corresponding to tickers
        flds: {list, string}
            String or list of strings corresponding to FLDS
        dates: list
            list of date strings in the format YYYYmmdd
        ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value. This should not include the date_field which will
            be iteratively overridden
        date_field: str
            Field to iteratively override for requesting historical data,
            e.g. REFERENCE_DATE, CURVE_DATE, etc.

        Example
        -------
        >>> import pdblp
        >>> con = pdblp.BCon()
        >>> con.start()
        >>> dates = ["20160625", "20160626"]
        >>> con.bulkref_hist("BVIS0587 Index", "CURVE_TENOR_RATES", dates,
        ...                  date_field="CURVE_DATE")

        """
        # correlationIDs should be unique to a session so rather than
        # managing unique IDs for the duration of the session just restart
        # a session for each call

        ovrds = [] if not ovrds else ovrds

        if type(tickers) is not list:
            tickers = [tickers]
        if type(flds) is not list:
            flds = [flds]
        self._send_hist(tickers, flds, dates, date_field, ovrds)
        data = self._parse_bulkref(flds, keep_corrId=True,
                                   sent_events=len(dates))
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'name', 'value', 'position', 'date']
        data = data.sort_values(by=['date', 'position']).reset_index(drop=True)
        data = data.loc[:, ['date', 'ticker', 'field', 'name',
                            'value', 'position']]
        return data

    def _send_hist(self, tickers, flds, dates, date_field, ovrds):
        logger = _get_logger(self.debug)
        self.restart()
        setvals = []
        request = self._create_req("ReferenceDataRequest", tickers, flds,
                                   ovrds, setvals)

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
            logger.info("Sending Request:\n %s" % request)
            self.session.sendRequest(request, correlationId=cid)

    def bdib(self, ticker, start_datetime, end_datetime, event_type, interval,
             elms=None):
        """
        Get Open, High, Low, Close, Volume, and numEvents for a ticker.
        Return pandas DataFrame

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
            to be set. Refer to the IntradayBarRequest section in the
            'Services & schemas reference guide' for more info on these values
        """
        elms = [] if not elms else elms

        # flush event queue in case previous call errored out
        logger = _get_logger(self.debug)
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

        logger.info("Sending Request:\n %s" % request)
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        # Process received events
        flds = ['open', 'high', 'low', 'close', 'volume', 'numEvents']
        while(True):
            ev = self.session.nextEvent(self.timeout)
            for msg in ev:
                logger.info("Message Received:\n %s" % msg)
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
        data = pd.DataFrame(data)
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
        logger = _get_logger(self.debug)
        request = self.exrService.createRequest("ExcelGetGridRequest")
        request.set("Domain", domain)
        self.session.sendRequest(request)
        data = []
        # Process received events
        while True:
            event = self.session.nextEvent(self.timeout)
            if event.eventType() == blpapi.Event.RESPONSE or \
               event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in event:
                    logger.info(msg)
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
