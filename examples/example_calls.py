import pdblp

# instantiate and start a connection, set debug=True for printing messages
con = pdblp.BCon(debug=True)
con.start()

# simple bdh call
con.bdh('SPY Equity', 'PX_LAST', '20150629', '20150630')

# two fields returns MultiIndex
con.bdh('SPY Equity', ['PX_LAST', 'VOLUME'], '20150629', '20150630')

# bdh with override
con.bdh('MPMIEZMA Index', 'PX_LAST', '20150101', '20150830')
con.bdh('MPMIEZMA Index', 'PX_LAST', '20150101', '20150830',
        ovrds=[('RELEASE_STAGE_OVERRIDE', 'P')])

# reference data request
con.ref('AUDUSD Curncy', 'SETTLE_DT')
con.ref(['NZDUSD Curncy', 'AUDUSD Curncy'], 'SETTLE_DT')
con.ref('AUDUSD Curncy', ['SETTLE_DT', 'DAYS_TO_MTY'])
con.ref(['NZDUSD Curncy', 'AUDUSD Curncy'], ['SETTLE_DT', 'DAYS_TO_MTY'])
con.ref('AUDUSD Curncy', 'SETTLE_DT', [('REFERENCE_DATE', '20150715')])
con.ref(['NZDUSD Curncy', 'AUDUSD Curncy'], ['SETTLE_DT', 'DAYS_TO_MTY'],
        [('REFERENCE_DATE', '20150715')])
# mutli reference data request
con.ref('W 1 Comdty', 'FUT_CHAIN')
con.ref('W 1 Comdty', 'FUT_CHAIN', [('INCLUDE_EXPIRED_CONTRACTS', 'Y')])

# custom request
request = con.refDataService.createRequest("ReferenceDataRequest")
request.append('securities', 'AUD1M Curncy')
request.append('fields', 'DAYS_TO_MTY')
con.custom_req(request)

# add field override
overrides = request.getElement('overrides')
override1 = overrides.appendElement()
override1.setElement("fieldId", 'REFERENCE_DATE')
override1.setElement('value', '20150629')
con.custom_req(request)

# historical reference data request
con.ref_hist('AUD1M Curncy', 'DAYS_TO_MTY', '20150625', '20150629')
con.ref_hist(['AUD1M Curncy', 'NZD1M Curncy'], 'DAYS_TO_MTY', '20150625',
             '20150629')
con.ref_hist('AUD1M Curncy', ['DAYS_TO_MTY', 'SETTLE_DT'], '20150625',
             '20150629')
con.ref_hist(['AUD1M Curncy', 'NZD1M Curncy'], ['DAYS_TO_MTY', 'SETTLE_DT'],
             '20150625', '20150629')
