import blpapi


def custom_req(session, request):
    """
    Utility for sending a predefined request and printing response as well
    as storing messages in a list, useful for testing

    Parameters
    ----------
    session: blpapi.session.Session
    request: blpapi.request.Request
        Request to be sent

    Returns
    -------
        List of all messages received
    """
    # flush event queue in case previous call errored out
    while(session.tryNextEvent()):
        pass

    print("Sending Request:\n %s" % request)
    session.sendRequest(request)
    messages = []
    # Process received events
    while(True):
        # We provide timeout to give the chance for Ctrl+C handling:
        ev = session.nextEvent(500)
        for msg in ev:
            print("Message Received:\n %s" % msg)
            messages.append(msg)
        if ev.eventType() == blpapi.Event.RESPONSE:
            # Response completely received, so we could exit
            break
    return messages
