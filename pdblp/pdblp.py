import logging
import contextlib

import blpapi
import numpy as np
import pandas as pd


_RESPONSE_TYPES = [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]

# partial lookup table for events used from blpapi.Event
_EVENT_DICT = {
              blpapi.Event.SESSION_STATUS: 'SESSION_STATUS',
              blpapi.Event.RESPONSE: 'RESPONSE',
              blpapi.Event.PARTIAL_RESPONSE: 'PARTIAL_RESPONSE',
              blpapi.Event.SERVICE_STATUS: 'SERVICE_STATUS',
              blpapi.Event.TIMEOUT: 'TIMEOUT',
              blpapi.Event.REQUEST: 'REQUEST'
}


def _get_logger(debug):
    logger = logging.getLogger(__name__)
    if (logger.parent is not None) and logger.parent.hasHandlers() and debug:
        logger.warning('"pdblp.BCon.debug=True" is ignored when user '
                       'specifies logging event handlers')
    else:
        if not logger.handlers:
            formatter = logging.Formatter('%(name)s:%(levelname)s:%(message)s')
            sh = logging.StreamHandler()
            sh.setFormatter(formatter)
            logger.addHandler(sh)
        debug_level = logging.INFO if debug else logging.WARNING
        logger.setLevel(debug_level)

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
    def __init__(self, host='localhost', port=8194, debug=False, timeout=500,
                 session=None, identity=None):
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
        session: blpapi.Session
            A custom Bloomberg API session. If this is passed the host and port
            parameters are ignored. This is exposed to allow the user more
            customization in how they instantiate a session.
        identity: blpapi.Identity
            Identity to use for request authentication. This should only be
            passed with an appropriate session and should already by
            authenticated. This is only relevant for SAPI and B-Pipe.
        """

        if session is None:
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost(host)
            sessionOptions.setServerPort(port)
            session = blpapi.Session(sessionOptions)
        else:
            ev = session.nextEvent(timeout)
            if ev.eventType() != blpapi.Event.TIMEOUT:
                raise ValueError('Flush event queue of blpapi.Session prior '
                                 'to instantiation')

        self.timeout = timeout
        self._session = session
        self._identity = identity
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
        Start connection and initialize session services
        """

        # flush event queue in defensive way
        logger = _get_logger(self.debug)
        started = self._session.start()
        if started:
            ev = self._session.nextEvent()
            ev_name = _EVENT_DICT[ev.eventType()]
            logger.info('Event Type: {!r}'.format(ev_name))
            for msg in ev:
                logger.info('Message Received:\n{}'.format(msg))
            if ev.eventType() != blpapi.Event.SESSION_STATUS:
                raise RuntimeError('Expected a "SESSION_STATUS" event but '
                                   'received a {!r}'.format(ev_name))
            ev = self._session.nextEvent()
            ev_name = _EVENT_DICT[ev.eventType()]
            logger.info('Event Type: {!r}'.format(ev_name))
            for msg in ev:
                logger.info('Message Received:\n{}'.format(msg))
            if ev.eventType() != blpapi.Event.SESSION_STATUS:
                raise RuntimeError('Expected a "SESSION_STATUS" event but '
                                   'received a {!r}'.format(ev_name))
        else:
            ev = self._session.nextEvent(self.timeout)
            if ev.eventType() == blpapi.Event.SESSION_STATUS:
                for msg in ev:
                    logger.warning('Message Received:\n{}'.format(msg))
                raise ConnectionError('Could not start blpapi.Session')
        self._init_services()
        return self

    def _init_services(self):
        """
        Initialize blpapi.Session services
        """
        logger = _get_logger(self.debug)

        # flush event queue in defensive way
        opened = self._session.openService('//blp/refdata')
        ev = self._session.nextEvent()
        ev_name = _EVENT_DICT[ev.eventType()]
        logger.info('Event Type: {!r}'.format(ev_name))
        for msg in ev:
            logger.info('Message Received:\n{}'.format(msg))
        if ev.eventType() != blpapi.Event.SERVICE_STATUS:
            raise RuntimeError('Expected a "SERVICE_STATUS" event but '
                               'received a {!r}'.format(ev_name))
        if not opened:
            logger.warning('Failed to open //blp/refdata')
            raise ConnectionError('Could not open a //blp/refdata service')
        self.refDataService = self._session.getService('//blp/refdata')

        opened = self._session.openService('//blp/exrsvc')
        ev = self._session.nextEvent()
        ev_name = _EVENT_DICT[ev.eventType()]
        logger.info('Event Type: {!r}'.format(ev_name))
        for msg in ev:
            logger.info('Message Received:\n{}'.format(msg))
        if ev.eventType() != blpapi.Event.SERVICE_STATUS:
            raise RuntimeError('Expected a "SERVICE_STATUS" event but '
                               'received a {!r}'.format(ev_name))
        if not opened:
            logger.warning('Failed to open //blp/exrsvc')
            raise ConnectionError('Could not open a //blp/exrsvc service')
        self.exrService = self._session.getService('//blp/exrsvc')

        return self

    def _create_req(self, rtype, tickers, flds, ovrds, setvals):
        # flush event queue in case previous call errored out
        while(self._session.tryNextEvent()):
            pass

        request = self.refDataService.createRequest(rtype)
        for t in tickers:
            request.getElement('securities').appendValue(t)
        for f in flds:
            request.getElement('fields').appendValue(f)
        for name, val in setvals:
            request.set(name, val)

        overrides = request.getElement('overrides')
        for ovrd_fld, ovrd_val in ovrds:
            ovrd = overrides.appendElement()
            ovrd.setElement('fieldId', ovrd_fld)
            ovrd.setElement('value', ovrd_val)

        return request

    def _receive_events(self, sent_events=1, to_dict=True):
        logger = _get_logger(self.debug)
        while True:
            ev = self._session.nextEvent(self.timeout)
            ev_name = _EVENT_DICT[ev.eventType()]
            logger.info('Event Type: {!r}'.format(ev_name))
            if ev.eventType() in _RESPONSE_TYPES:
                for msg in ev:
                    logger.info('Message Received:\n{}'.format(msg))
                    if to_dict:
                        yield message_to_dict(msg)
                    else:
                        yield msg

            # deals with multi sends using CorrelationIds
            if ev.eventType() == blpapi.Event.RESPONSE:
                sent_events -= 1
                if sent_events == 0:
                    break
            # guard against unknown returned events
            elif ev.eventType() not in _RESPONSE_TYPES:
                logger.warning('Unexpected Event Type: {!r}'.format(ev_name))
                for msg in ev:
                    logger.warning('Message Received:\n{}'.format(msg))
                if ev.eventType() == blpapi.Event.TIMEOUT:
                    raise RuntimeError('Timeout, increase BCon.timeout '
                                       'attribute')
                else:
                    raise RuntimeError('Unexpected Event Type: {!r}'
                                       .format(ev_name))

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

        df = pd.DataFrame(data, columns=['date', 'ticker', 'field', 'value'])
        df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'date'])
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
        setvals.append(('startDate', start_date))
        setvals.append(('endDate', end_date))

        request = self._create_req('HistoricalDataRequest', tickers, flds,
                                   ovrds, setvals)
        logger.info('Sending Request:\n{}'.format(request))
        # Send the request
        self._session.sendRequest(request, identity=self._identity)
        data = []
        # Process received events
        for msg in self._receive_events():
            d = msg['element']['HistoricalDataResponse']

            if 'responseError' in d:
                raise ValueError(d)

            has_security_error = 'securityError' in d['securityData']
            has_field_exception = len(d['securityData']['fieldExceptions']) > 0
            if has_security_error or has_field_exception:
                raise ValueError(d)
            ticker = d['securityData']['security']
            fldDatas = d['securityData']['fieldData']
            for fd in fldDatas:
                for fname, value in fd['fieldData'].items():
                    if fname == 'date':
                        continue
                    data.append(
                        (fd['fieldData']['date'], ticker, fname, value)
                    )
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
        request = self._create_req('ReferenceDataRequest', tickers, flds,
                                   ovrds, [])
        logger.info('Sending Request:\n{}'.format(request))
        self._session.sendRequest(request, identity=self._identity)
        data = self._parse_ref(flds)
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'value']
        return data

    def _parse_ref(self, flds, keep_corrId=False, sent_events=1):
        data = []
        # Process received events
        for msg in self._receive_events(sent_events):
            if keep_corrId:
                corrId = msg['correlationIds']
            else:
                corrId = []
            d = msg['element']['ReferenceDataResponse']

            if 'responseError' in d:
                raise ValueError(d)

            for security_data_dict in d:
                secData = security_data_dict['securityData']
                ticker = secData['security']
                if 'securityError' in secData:
                    raise ValueError('Unknow security {!r}'.format(ticker))
                self._check_fieldExceptions(secData['fieldExceptions'])
                fieldData = secData['fieldData']['fieldData']
                for fld in flds:
                    # avoid returning nested bbg objects, fail instead
                    # since user should use bulkref()
                    if (fld in fieldData) and isinstance(fieldData[fld], list):
                        raise ValueError('Field {!r} returns bulk reference '
                                         'data which is not supported'
                                         .format(fld))
                    # this is a slight hack but if a fieldData response
                    # does not have the element fld and this is not a bad
                    # field (which is checked above) then the assumption is
                    # that this is a not applicable field, thus set NaN
                    # see https://github.com/matthewgilbert/pdblp/issues/13
                    if fld not in fieldData:
                        datum = [ticker, fld, np.NaN]
                        datum.extend(corrId)
                        data.append(datum)
                    else:
                        val = fieldData[fld]
                        datum = [ticker, fld, val]
                        datum.extend(corrId)
                        data.append(datum)
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
        request = self._create_req('ReferenceDataRequest', tickers, flds,
                                   ovrds, setvals)
        logger.info('Sending Request:\n{}'.format(request))
        self._session.sendRequest(request, identity=self._identity)
        data = self._parse_bulkref(flds)
        data = pd.DataFrame(data)
        data.columns = ['ticker', 'field', 'name', 'value', 'position']
        return data

    def _parse_bulkref(self, flds, keep_corrId=False, sent_events=1):
        data = []
        # Process received events
        for msg in self._receive_events(sent_events):
            if keep_corrId:
                corrId = msg['correlationIds']
            else:
                corrId = []
            d = msg['element']['ReferenceDataResponse']

            if 'responseError' in d:
                raise ValueError(d)

            for security_data_dict in d:
                secData = security_data_dict['securityData']
                ticker = secData['security']
                if 'securityError' in secData:
                    raise ValueError('Unknow security {!r}'.format(ticker))
                self._check_fieldExceptions(secData['fieldExceptions'])
                fieldData = secData['fieldData']['fieldData']
                for fld in flds:
                    # fail coherently instead of while parsing downstream
                    if (fld in fieldData) and not isinstance(fieldData[fld], list): # NOQA
                        raise ValueError('Cannot parse field {!r} which is '
                                         'not bulk reference data'.format(fld))
                    elif fld in fieldData:
                        for i, data_dict in enumerate(fieldData[fld]):
                            for name, value in data_dict[fld].items():
                                datum = [ticker, fld, name, value, i]
                                datum.extend(corrId)
                                data.append(datum)
                    else:  # field is empty or NOT_APPLICABLE_TO_REF_DATA
                        datum = [ticker, fld, np.NaN, np.NaN, np.NaN]
                        datum.extend(corrId)
                        data.append(datum)
        return data

    @staticmethod
    def _check_fieldExceptions(field_exceptions):
        # iterate over an array of field_exceptions and check for a
        # INVALID_FIELD error
        for fe_dict in field_exceptions:
            fe = fe_dict['fieldExceptions']
            if fe['errorInfo']['errorInfo']['subcategory'] == 'INVALID_FIELD':
                raise ValueError('{}: INVALID_FIELD'.format(fe['fieldId']))

    def ref_hist(self, tickers, flds, dates, ovrds=None,
                 date_field='REFERENCE_DATE'):
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
                     date_field='REFERENCE_DATE'):
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
        setvals = []
        request = self._create_req('ReferenceDataRequest', tickers, flds,
                                   ovrds, setvals)

        overrides = request.getElement('overrides')
        if len(dates) == 0:
            raise ValueError('dates must by non empty')
        ovrd = overrides.appendElement()
        for dt in dates:
            ovrd.setElement('fieldId', date_field)
            ovrd.setElement('value', dt)
            # CorrelationID used to keep track of which response coincides with
            # which request
            cid = blpapi.CorrelationId(dt)
            logger.info('Sending Request:\n{}'.format(request))
            self._session.sendRequest(request, identity=self._identity,
                                      correlationId=cid)

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
        while(self._session.tryNextEvent()):
            pass

        # Create and fill the request for the historical data
        request = self.refDataService.createRequest('IntradayBarRequest')
        request.set('security', ticker)
        request.set('eventType', event_type)
        request.set('interval', interval)  # bar interval in minutes
        request.set('startDateTime', start_datetime)
        request.set('endDateTime', end_datetime)
        for name, val in elms:
            request.set(name, val)

        logger.info('Sending Request:\n{}'.format(request))
        # Send the request
        self._session.sendRequest(request, identity=self._identity)
        # Process received events
        data = []
        flds = ['open', 'high', 'low', 'close', 'volume', 'numEvents']
        for msg in self._receive_events():
            d = msg['element']['IntradayBarResponse']
            for bar in d['barData']['barTickData']:
                data.append(bar['barTickData'])
        data = pd.DataFrame(data).set_index('time').sort_index().loc[:, flds]
        return data

    def bsrch(self, domain):
        """
        This function uses the Bloomberg API to retrieve 'bsrch' (Bloomberg
        SRCH Data) queries. Returns list of tickers.

        Parameters
        ----------
        domain: string
            A character string with the name of the domain to execute.
            It can be a user defined SRCH screen, commodity screen or
            one of the variety of Bloomberg examples. All domains are in the
            format <domain>:<search_name>. Example "COMDTY:NGFLOW"

        Returns
        -------
        data: pandas.DataFrame
            List of bloomberg tickers from the BSRCH
        """
        logger = _get_logger(self.debug)
        request = self.exrService.createRequest('ExcelGetGridRequest')
        request.set('Domain', domain)
        logger.info('Sending Request:\n{}'.format(request))
        self._session.sendRequest(request, identity=self._identity)
        data = []
        for msg in self._receive_events(to_dict=False):
            for v in msg.getElement("DataRecords").values():
                for f in v.getElement("DataFields").values():
                    data.append(f.getElementAsString("StringValue"))
        return pd.DataFrame(data)

    def stop(self):
        """
        Close the blp session
        """
        self._session.stop()


def _element_to_dict(elem):
    if isinstance(elem, str):
        return elem
    dtype = elem.datatype()
    if dtype == blpapi.DataType.CHOICE:
        return {str(elem.name()): _element_to_dict(elem.getChoice())}
    elif elem.isArray():
        return [_element_to_dict(v) for v in elem.values()]
    elif dtype == blpapi.DataType.SEQUENCE:
        return {str(elem.name()): {str(e.name()): _element_to_dict(e) for e in elem.elements()}}  # NOQA
    else:
        if elem.isNull():
            value = None
        else:
            try:
                value = elem.getValue()
            except:  # NOQA
                value = None
        return value


def message_to_dict(msg):
    return {
        'correlationIds': [cid.value() for cid in msg.correlationIds()],
        'messageType': "{}".format(msg.messageType()),
        'topicName': msg.topicName(),
        'element': _element_to_dict(msg.asElement())
    }
