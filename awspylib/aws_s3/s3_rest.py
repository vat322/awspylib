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
from awspylib.aws_genutilities import TIME_FORMAT

DEFAULT_HOST = 's3.amazonaws.com'
PORTS_BY_SECURITY = { True: 443, False: 80 }
METADATA_PREFIX = 'x-amz-meta-'
AMAZON_HEADER_PREFIX = 'x-amz-'

commLog  = logging.getLogger('comm')

# generates the aws canonical string for the given parameters
def canonical_string(method, bucket="", key="", query_args={}, headers={}, expires=None):
    interesting_headers = {}
    for header_key in headers:
        lk = header_key.lower()
        if lk in ['content-md5', 'content-type', 'date'] or lk.startswith(AMAZON_HEADER_PREFIX):
            interesting_headers[lk] = headers[header_key].strip()

    # these keys get empty strings if they don't exist
    if not interesting_headers.has_key('content-type'):
        interesting_headers['content-type'] = ''
    if not interesting_headers.has_key('content-md5'):
        interesting_headers['content-md5'] = ''

    # just in case someone used this.  it's not necessary in this lib.
    if interesting_headers.has_key('x-amz-date'):
        interesting_headers['date'] = ''

    # if you're using expires for query string auth, then it trumps date
    # (and x-amz-date)
    if expires:
        interesting_headers['date'] = str(expires)

    sorted_header_keys = interesting_headers.keys()
    sorted_header_keys.sort()

    buf = "%s\n" % method
    for header_key in sorted_header_keys:
        if header_key.startswith(AMAZON_HEADER_PREFIX):
            buf += "%s:%s\n" % (header_key, interesting_headers[header_key])
        else:
            buf += "%s\n" % interesting_headers[header_key]

    # append the bucket if it exists
    if bucket != "":
        buf += "/%s" % bucket

    # add the key.  even if it doesn't exist, add the slash
    buf += "/%s" % urllib.quote_plus(key)

    # handle special query string arguments

    if query_args.has_key("acl"):
        buf += "?acl"
    elif query_args.has_key("torrent"):
        buf += "?torrent"
    elif query_args.has_key("logging"):
        buf += "?logging"

    return buf

# computes the base64'ed hmac-sha hash of the canonical string and the secret
# access key, optionally urlencoding the result
def encode(aws_secret_access_key, str, urlencode=False):
    b64_hmac = base64.encodestring(hmac.new(aws_secret_access_key, str, sha).digest()).strip()
    if urlencode:
        return urllib.quote_plus(b64_hmac)
    else:
        return b64_hmac

def merge_meta(headers, metadata):
    final_headers = headers.copy()
    for k in metadata.keys():
        final_headers[METADATA_PREFIX + k] = metadata[k]

    return final_headers

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


class CallingFormat:
    REGULAR = 1
    SUBDOMAIN = 2
    VANITY = 3

    def build_url_base(protocol, server, port, bucket, calling_format):
        url_base = '%s://' % protocol

        if bucket == '':
            url_base += server
        elif calling_format == CallingFormat.SUBDOMAIN:
            url_base += "%s.%s" % (bucket, server)
        elif calling_format == CallingFormat.VANITY:
            url_base += bucket
        else:
            url_base += server

        url_base += ":%s" % port

        if (bucket != '') and (calling_format == CallingFormat.REGULAR):
            url_base += "/%s" % bucket

        return url_base

    build_url_base = staticmethod(build_url_base)



class AWSAuthConnection:
    def __init__(self, aws_access_key_id, aws_secret_access_key, is_secure=True,
            server=DEFAULT_HOST, port=None, calling_format=CallingFormat.REGULAR):

        if not port:
            port = PORTS_BY_SECURITY[is_secure]

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.is_secure = is_secure
        self.server = server
        self.port = port
        self.calling_format = calling_format
        self.path = ''
        self.final_headers  = {}
        
        if (commLog.level == logging.DEBUG):
            httplib.HTTPConnection.debuglevel = 1
    
    def prepare_message(self, method, bucket='', key='', query_args={}, headers={}, metadata={}):

        server = ''
        if bucket == '':
            server = self.server
        elif self.calling_format == CallingFormat.SUBDOMAIN:
            server = "%s.%s" % (bucket, self.server)
        elif self.calling_format == CallingFormat.VANITY:
            server = bucket
        else:
            server = self.server

        self.path = ''

        if (bucket != '') and (self.calling_format == CallingFormat.REGULAR):
            self.path += "/%s" % bucket

        # add the slash after the bucket regardless
        # the key will be appended if it is non-empty
        self.path += "/%s" % urllib.quote_plus(key)


        # build the path_argument string
        # add the ? in all cases since 
        # signature and credentials follow path args
        self.path += "?"
        self.path += query_args_hash_to_string(query_args)

        self.final_headers = merge_meta(headers, metadata);
        # add auth header
        self.add_aws_auth_header(self.final_headers, method, bucket, key, query_args)
        
        return


    def make_request(self, method, bucket='', key='', query_args={}, headers={}, data='', metadata={}):

        try:
            self.prepare_message ( method, bucket, key, query_args, headers, metadata )
        
            if (self.is_secure):
                connection = httplib.HTTPSConnection("%s:%d" % (self.server, self.port))
            else:
                connection = httplib.HTTPConnection("%s:%d" % (self.server, self.port))
        

            commLog.debug('RQST MSG:%s --details follow--\n\t%s - %s:%d\n\tHeaders:%s' % \
                          (method, self.path, connection.host, connection.port, self.final_headers) )

                                    
            connection.request(method, self.path, data, self.final_headers)
            response = connection.getresponse( )
        

            commLog.debug('RESP MSG: Status=%d(%s).  ---msg follows---\n%s' % \
                       (response.status, response.reason, response.msg) )

            if (response.status >= 400 ):
                # We have a bad HTTP response
                raise S3Error ( response.status, response.reason, method, bucket, key)
            
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


    def add_aws_auth_header(self, headers, method, bucket, key, query_args):
        if not headers.has_key('Date'):
            headers['Date'] = time.strftime(TIME_FORMAT, time.gmtime())

        c_string = canonical_string(method, bucket, key, query_args, headers)            
        headers['Authorization'] = \
            "AWS %s:%s" % (self.aws_access_key_id, encode(self.aws_secret_access_key, c_string))

        commLog.debug('canonical string: %r\nAuthN: %s' % (c_string, headers['Authorization'] ) )