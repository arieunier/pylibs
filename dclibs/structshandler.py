from typing import Any
from dataclasses import dataclass
from dclibs import utils, config, sfapi, logs
import json

LOGGER = logs.LOGGER


@dataclass
class CustomerStructure:
    origin: str
    changetype: str
    salesforceid: str

    firstname: str
    lastname:str
    email: str
    stripeid: str
    
    ispersonaccount: str
    isstripeuser: str
    relatedaccountid: str

    objecttype: str

    @staticmethod 
    def from_dict_Salesforce(obj: Any) -> 'CustomerStructure':
        _origin = 'SALESFORCE'
        _changetype = obj.get("data").get("payload").get("ChangeEventHeader").get("changeType")
        _salesforceid = obj.get("data").get("payload").get("ChangeEventHeader").get("recordIds")[0]
        _firstname=None
        _lastname=None
        _email=None
        _stripeid=None
        _istripeuser=None
        _ispersonaccount=None
        _objecttype='Contact' # account if person account, contact otherwise
        # if create then we must have firstname lastname email ispersonaccount fields
        if _changetype == 'CREATE':
            _firstname=obj.get("data").get("payload").get("Name").get("FirstName")
            _lastname=obj.get("data").get("payload").get("Name").get("LastName")
            _email=obj.get("data").get("payload").get("Email")
            _ispersonaccount=obj.get("data").get("payload").get("IsPersonAccount")
            _istripeuser=obj.get("data").get("payload").get("isStripeUser__c")
            
        elif _changetype == 'UPDATE':
            _email = obj.get("data").get("payload").get("Email") if "Email" in obj.get("data").get("payload") else None
            _firstname = obj.get("data").get("payload").get("Name").get("FirstName") if "Name" in obj.get("data").get("payload") else None
            _lastname = obj.get("data").get("payload").get("Name").get("LastName") if "Name" in obj.get("data").get("payload") else None
            _istripeuser=obj.get("data").get("payload").get("isStripeUser__c")
            _ispersonaccount=obj.get("data").get("payload").get("IsPersonAccount")
            
        _relatedaccountid=obj.get("data").get("payload").get("AccountId")
        
        if (_ispersonaccount == True):
            _objecttype='Account'

        return CustomerStructure(_origin, _changetype, _salesforceid, _firstname, _lastname, _email, _stripeid,_ispersonaccount,_istripeuser,_relatedaccountid,_objecttype)


def Customer_refreshFromSalesforce(eventcontent):
    # calls Salesforce to get the proper data content because we 
    jwt_token=utils.get_jwt_token()
    SOQLEnd = "Account" if eventcontent.salesforceid.startswith(config.PREFIX_ACCOUNT) else "Contact"
    sfcontent = sfapi.sf_executeQuery(jwt_token['instance_url'],jwt_token['access_token'],None,
        "select id, firstname, lastname, email from  " + SOQLEnd + " where id = '{}'".format(eventcontent.salesforceid)).json()
    LOGGER.info(sfcontent)
    eventcontent.firstname = sfcontent['records'][0]['FirstName']
    eventcontent.lastname = sfcontent['records'][0]['LastName']
    eventcontent.email = sfcontent['records'][0]['Email']

def Customer_updateSalesforceRecord(eventcontent):
    jwt_token=utils.get_jwt_token()
    uid = eventcontent.salesforceid
    sfobject = "Account" if eventcontent.salesforceid.startswith(config.PREFIX_ACCOUNT) else "Contact"
    params = {'FirstName' : eventcontent.firstname,
    'LastName' : eventcontent.lastname,
    'Email':eventcontent.email,
    'Stripe_UID__c' : eventcontent.stripeid}
    sfapi.sf_updateRecord(jwt_token['instance_url'],jwt_token['access_token'], sfobject, uid , json.dumps(params))

"""
import json
#create use case
jsonstring = {'data': {'schema': 'RXBLZdHoO1a5VsAkYfcs_w', 'payload': {'LastModifiedDate': '2022-09-28T08:15:36.000Z', 'AccountId': '0011x00001fBFXwAAO', 'Email': 'augustin@stripe.com', 'Name': {'FirstName': 'Augustin', 'LastName': 'Rieunier', 'Salutation': 'Mr.'}, 'OwnerId': '0051x00000B2EceAAF', 'CreatedById': '0051x00000B2EceAAF', 'IsPersonAccount': True, 'CleanStatus': 'Pending', 'ChangeEventHeader': {'commitNumber': 686107207336, 'commitUser': '0051x00000B2EceAAF', 'sequenceNumber': 1, 'entityName': 'Contact', 'changeType': 'CREATE', 'changedFields': [], 'changeOrigin': 'com/salesforce/api/soap/55.0;client=SfdcInternalAPI/', 'transactionKey': '00058039-1c3d-26f0-e480-ccb60d9c1780', 'commitTimestamp': 1664352937000, 'recordIds': ['0031x00001OXjxiAAD']}, 'CreatedDate': '2022-09-28T08:15:36.000Z', 'LastModifiedById': '0051x00000B2EceAAF'}, 'event': {'replayId': 15421458}}, 'channel': '/data/ChangeEvents'}
struct = CustomerStructure.from_dict_Salesforce(jsonstring)
print(struct)
#update use case
jsonstring = {'data': {'schema': 'RXBLZdHoO1a5VsAkYfcs_w', 'payload': {'LastModifiedDate': '2022-09-28T08:29:03.000Z', 'Email': 'augustin+sftest@stripe.com', 'ChangeEventHeader': {'commitNumber': 686114461474, 'commitUser': '0051x00000B2EceAAF', 'sequenceNumber': 1, 'entityName': 'Contact', 'changeType': 'UPDATE', 'changedFields': ['Email', 'LastModifiedDate'], 'changeOrigin': 'com/salesforce/api/soap/55.0;client=SfdcInternalAPI/', 'transactionKey': '000580f4-e507-ade9-a22c-fbfb95152b12', 'commitTimestamp': 1664353743000, 'recordIds': ['0031x00001OXjxiAAD']}}, 'event': {'replayId': 15421532}}, 'channel': '/data/ChangeEvents'} 
struct = CustomerStructure.from_dict_Salesforce(jsonstring)
print(struct)
"""