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


from awspylib.aws_exception import *
from awspylib.aws_config import Config
import awspylib.aws_genutilities as Util
import awspylib.aws_sdb.sdb_util as SDBUtil
            
def test_sdb():
        
    conn = SDBUtil.SDB.AWSAuthConnection(Config.AWSProperties['AccessKey'], \
                                         Config.AWSProperties['SecretKey'], \
                                         ( Config.AWSProperties['SecureComm'] == True))

    simple_db = SDBUtil.AWS_SDB ( conn )
    
    simple_db.get_list_of_domains( )
    
    for domain in simple_db.ListOfDomains:
        simple_db.delete_domain (domain)
 
    simple_db.get_list_of_domains( )


    test = 'TEST-1'
    print '%s: Getting List of Domains' % test
    try:
        simple_db.get_list_of_domains( )
    except Exception, e:
        print '\t%s: - FAILED! (%s)' % (test, e)
    else:
        print '\t%s: - Success!' % test

    test = 'TEST-2'
    domain_name = 'kvb-test-domain-1234'
    print '%s: Creating a new domain' % test
    try:
        simple_db.add_domain( domain_name )
        simple_db.add_domain (domain_name+'56')
    except Exception, e:
        print '\t%s: - FAILED! (%s)' % (test, e)
    else:
        print '\t%s: - Success!' % test
    
    test = 'TEST-3'
    print '%s: Get list of domains and delete them' % test
    try:
        simple_db.get_list_of_domains( )
        for domain in simple_db.ListOfDomains:
            simple_db.delete_domain (domain)
        
    except Exception, e:
        print '\t%s: - FAILED! (%s)' % (test, e)
    else:
        print '\t%s: - Success!' % test

    return
def perftest_sdb( ):

    return

    
if __name__ == '__main__':
    
    test_sdb()
