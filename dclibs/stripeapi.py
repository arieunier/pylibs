#! /usr/bin/env python3.6

"""
server.py
Stripe Sample. 
Python 3.6 or newer required.
"""
import email
import requests
from dclibs import logs, structshandler

from importlib.metadata import metadata
from locale import currency
from pdb import post_mortem
from random import randrange
from xml.dom import ValidationErr
import stripe
import json
import os, logging
import random, string
import copy

from dotenv import load_dotenv, find_dotenv
LOGGER = logs.LOGGER
load_dotenv(find_dotenv('stripe.env'))
LOGGER.info(os.environ)



stripe.api_version = '2020-08-27'
stripe.api_key = os.getenv('STRIPE_SECRET_KEY')


def updateCustomer(id, struct):
    params = {}
    if (struct.firstname != None and struct.lastname != None):
        LOGGER.info("bingo both name changed")
        params["name"] = struct.firstname + " " + struct.lastname

    elif (struct.firstname != None or struct.lastname != None):
        LOGGER.info("need to get the new name")
        structshandler.Customer_refreshFromSalesforce(struct)
        params["name"] = struct.firstname + " " + struct.lastname

    if struct.email != None:
        params['email'] =struct.email 
    LOGGER.info(params)
    stripe.Customer.modify(sid=id,**params)

def getCustomerFromSFId(struct):
    params={
        'query' :'metadata["salesforceid"]:"{}"'.format(struct.salesforceid)
    }
    customer = stripe.Customer.search(**params)
    LOGGER.info(customer)
    if (len(customer.data) != 1):
        LOGGER.warn("could not find a unique customer in stripe")
        return None
    return customer.data[0]

def createCustomer(struct):
    name = struct.firstname + " " + struct.lastname
    params = {
        'name' : name,
        'email': struct.email ,
        'metadata' : {'salesforceid':struct.salesforceid, 'relatedaccountid':struct.relatedaccountid}
    }
    customer = stripe.Customer.create(**params)
    #LOGGER.info(customer)
    return customer