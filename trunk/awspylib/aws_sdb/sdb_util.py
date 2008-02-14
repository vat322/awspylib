#!/usr/bin/env python
# Copyright (c) <2008> <Kiran Bhageshpur - kiran_bhageshpur at hotmail dot com>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import sys
import os
import os.path
import time
import string
import httplib
import md5
import xml.sax
import logging


from awspylib.aws_exception import *
from awspylib.aws_config import Config
import awspylib.aws_sdb.sdb_rest as SDB
import awspylib.aws_genutilities as Util

_NUMBER_OF_RETRIES_ = 3

appLog  = logging.getLogger('app')
cbackLog = logging.getLogger('app.cback')

doc=""" 
This module contains various utility functions and classes 
required to interact with amazon S3
"""

def _default_cb_( method, bucket, key, length):
    cbackLog.info( '%s %s %s %d' % (method, bucket, key, length) )

class _S3Object_:
    def __init__(self ):
        self.data = data
        self.metadata = metadata

class _Response_:
    def __init__(self, http_response):
        self.body = ''
        self.http_response = http_response
        # you have to do this read, even if you don't expect a body.
        # otherwise, the next request fails.
        if ( http_response.status < 300 ):
            self.body = http_response.read( )

    def _get_header_( self, header ):
        return (self.http_response.getheader( header ) )

    def _get_msg_( self ):
        return (self.http_response.msg )

    def _get_status_ ( self ):
        return (self.http_response.status)

    def _get_reason_( self ):
        return (self.http_response.reason)

    def _get_version_( self ):
        return (self.http_response.version)        

    def _get_date_ ( self ):
        return (self._get_header_('date') )

    def _get_last_modified_( self ):
        return (self._get_header_('last-modified') )

    def _get_etag_ (self ):
        return (self._get_header_('etag').strip('\"') )

    def _get_content_type_(self):
        return (self._get_header_('content-type') )

    def _get_content_length_(self):
        return (int( self._get_header_('content-length') ) )

    def _get_server_(self):
        return (self._get_header_('server'))


class _Response_Metadata_:
    def __init__(self, request_id='', usage=''):
        self.request_id = request_id
        self.usage      = usage

class AWS_SDB:
    doc = """
        This class encapsulates operations on AWS SimpleDB.

        The method naming convention is all methods starting with lower case
        are lower level methods that use sdb_rest::AWSConnectionObject to 
        interact with AWS.
        """
    def __init__(self, AWSConnectionObject):
        doc = """
            AWSConnObject: An instance of AWSConnectionObject used to communicate with S3
            """
        self.CONN = AWSConnectionObject
        self.ListOfDomains = []
        self.response_meta_data = None
        return
    
    class _ListOfAllDomainsResponse_(_Response_):
        def __init__(self, http_response):
            _Response_.__init__(self, http_response)
            if self._get_status_() < 300: 
                handler = self._ListOfAllDomainsHandler_()
                xml.sax.parseString(self.body, handler)
                self.domains = handler.domains
                self.response_meta_data = handler.response_meta_data
            else:
                self.domains = []
                self.response_meta_data = None

        class _ListOfAllDomainsHandler_(xml.sax.ContentHandler):
            def __init__(self):
                self.domains = []
                self.response_meta_data = None
                self.next_token = ''
                self.curr_text = ''

            def startElement(self, name, attrs):
                if name == 'ResponseMetadata':
                    self.response_meta_data = _Response_Metadata_( )

            def endElement(self, name):
                if name == 'DomainName':
                    self.domains.append ( self.curr_text )
                elif name == 'NextToken':
                    self.next_token = self.curr_text
                elif name == 'RequestId':
                    self.response_meta_data.request_id = self.curr_text
                elif name == 'BoxUsage':
                    self.response_meta_data.usage = self.curr_text

            def characters(self, content):
                self.curr_text = content


    def get_domain_name ( self, index ):
        return ( self.ListOfDomains[index].name )

    def get_list_of_domains( self ):
        doc="""
           Retrieve the list of all domains owned by the credentials used to construct the AWSConnectionObject
           contained herein
           """
        try:
            resp = self._ListOfAllDomainsResponse_( self.CONN.make_request ( action='ListDomains', query_args={}) )
            self.ListOfDomains = resp.domains
            self.response_meta_data = resp.response_meta_data

        except Exception, e:
            logging.error ('ERROR %s' % e)
            raise

        return

    def add_domain( self, domainName ):
        doc="""
           Add a new domain with the name domainName
           """
        try:
            resp = _Response_ ( self.CONN.make_request ( action='CreateDomain', \
                                                         query_args={'DomainName':domainName} ) )

        except Exception, e:
            logging.error ('ERROR %s' % e)
            raise

        return


    def delete_domain ( self, domainName):
        doc="""
           Delete domain bucket and all items within it
           """
        try:
            resp = _Response_ ( self.CONN.make_request (action='DeleteDomain', \
                                                        query_args={'DomainName':domainName} ) )

        except Exception, e:
            logging.error ('ERROR %s' % e)
            raise

        return

    

if __name__ == '__main__':
    
    conn = SDB.AWSAuthConnection ( Config.AWSProperties['AccessKey'], Config.AWSProperties['SecretKey'], False)

    simple_db = AWS_SDB ( conn )
    
    simple_db.get_list_of_domains( )
    
    for domain in simple_db.ListOfDomains:
        simple_db.delete_domain (domain)
 
    simple_db.get_list_of_domains( )
    