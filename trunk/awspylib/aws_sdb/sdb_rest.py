#!/usr/bin/env python

#  This software code is made available "AS IS" without warranties of any
#  kind.  You may copy, display, modify and redistribute the software
#  code either by itself or as incorporated into your code; provided that
#  you do not remove any proprietary notices.  Your use of this software
#  code is at your own risk and you waive any claim against Amazon
#  Digital Services, Inc. or its affiliates with respect to your use of
#  this software code. (c) 2006 Amazon Digital Services, Inc. or its
#  affiliates.

import base64
import hmac
import httplib
import re
import sha
import sys
import time
import urllib
import socket
import xml.sax
import logging

from awspylib.aws_exception import *
from awspylib.aws_config import Config
from awspylib.aws_genutilities import SDB_TIME_FORMAT, sort_dictionary

DEFAULT_HOST = 'sdb.amazonaws.com'
PORTS_BY_SECURITY = { True: 443, False: 80 }

commLog  = logging.getLogger('comm')


# computes the base64'ed hmac-sha hash of the canonical string and the secret
# access key, optionally urlencoding the result
def encode(aws_secret_access_key, str, urlencode=False):
    b64_hmac = base64.encodestring(hmac.new(aws_secret_access_key, str, sha).digest()).strip()
    if urlencode:
        return urllib.quote_plus(b64_hmac)
    else:
        return b64_hmac

# builds the query arg string
def query_args_hash_to_string(query_args):
    query_string = ""
    pairs = []
    for k, v in query_args.items():
        piece = k
        if v != None:
            piece += "=%s" % urllib.quote_plus(str(v))
        pairs.append(piece)

    return '&'.join(pairs)


class AWSAuthConnection:
    SIGNATURE_VERSION = '1'
    API_VERSION = '2007-11-07'
    def __init__(self, aws_access_key_id, aws_secret_access_key, is_secure=True,
            server=DEFAULT_HOST, port=None ):

        if not port:
            port = PORTS_BY_SECURITY[is_secure]

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.is_secure = is_secure
        self.server = server
        self.port = port
        self.path = ''
        
        if (commLog.level == logging.DEBUG):
            httplib.HTTPConnection.debuglevel = 1
    
    def prepare_message(self, action, query_args={}):

        # TODO - Prep arguments to ensure that there are no "illegal" characters

                
        # Add the Action, Signature Version, Timestamp & access key
        query_args['Action'] = action
        query_args['SignatureVersion'] = self.SIGNATURE_VERSION
        query_args['Version'] = self.API_VERSION
        query_args['Timestamp'] = time.strftime(SDB_TIME_FORMAT, time.gmtime())
        query_args['AWSAccessKeyId'] = self.aws_access_key_id
        
        # Sort request parameters alphabetically
        sorted_query_args = sort_dictionary ( query_args )

        #Create string to sign
        string_to_sign = ''
        for item in sorted_query_args:
            string_to_sign = string_to_sign+str(item[0])+str(item[1])
        
        # compute and add signature to query args
        query_args['Signature'] = encode(self.aws_secret_access_key, string_to_sign)
        
        # Build the path argument string
        self.path = '/?'
        self.path += query_args_hash_to_string(query_args)
                
        return


    def make_request(self, action, query_args={} ):

        try:
            self.prepare_message ( action, query_args)
        
            if (self.is_secure):
                connection = httplib.HTTPSConnection("%s:%d" % (self.server, self.port))
            else:
                connection = httplib.HTTPConnection("%s:%d" % (self.server, self.port))
        

            commLog.debug('RQST MSG:%s, %s:%d \n\tPath:%s' % (action, connection.host, connection.port, self.path) )
                        
            connection.request('GET', self.path )
            response = connection.getresponse( )
        
            commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                       (response.status, response.reason, response.msg) )

            if (response.status >= 400 ):
                # We have a bad HTTP response
                raise SDBError ( response.status, response.reason, action)
            
        except socket.error, (value, message):
            logging.error('Caught %d:%s. Aborting' % (value, message) ) 
            raise
        except S3Error, e:
            logging.error(e.__str__() )
            raise
        except Exception, f:
            logging.error ('ERROR %s' % f)
            raise

        return response

    
if __name__ == '__main__':
    
    conn = AWSAuthConnection ( Config.AWSProperties['AccessKey'], Config.AWSProperties['SecretKey'], False)

    resp = conn.make_request( 'ListDomains' )
    resp_data = resp.read()
    
    resp = conn.make_request ( 'CreateDomain', {'DomainName':'kvb-test-domain'} )
    resp_data = resp.read()
    
    print resp