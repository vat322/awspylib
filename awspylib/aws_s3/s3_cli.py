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
import string
import time
import logging

from awspylib.aws_config import Config
from awspylib.aws_exception import *
import awspylib.aws_s3.s3_util as S3Util
import awspylib.aws_genutilities as Util


doc=""" 
%prog [Options] [inputfile [outputfile]]
"""

class Bucket_CLI ( S3Util.AWS_S3 ):

    def CLI_ListBuckets( self, fGetInput = False ):
        self.get_list_of_buckets( )
        numEntries = Util.printList (list = self.ListOfBuckets)
        if ( fGetInput == True ):
            return (Util.getDigitInput ( 0, numEntries ) )

        return

    def CLI_AddBucket( self ):
        newBucket = Util.getStringInput ( 'Enter name of bucket-->')
        self.add_bucket( newBucket )
        self.CLI_ListBuckets ( )
        return


    def CLI_DeleteBucket( self ):
        userSelection = self.CLI_ListBuckets ( True )
        self.delete_bucket ( self.ListOfBuckets[userSelection].name  )
        self.CLI_ListBuckets ( False)
        return

    def CLI_DestroyBucket ( self):
        userSelection = self.CLI_ListBuckets ( True )
        prompt = 'WARNING! All contents of bucket=<%s> will be deleted. Enter <q> to quit' % \
               self.ListOfBuckets[userSelection].name
        ui = raw_input ( prompt )
        if ( ui == 'q'):
            return
        self.delete_bucket_recursive ( self.ListOfBuckets[userSelection].name )
        self.CLI_ListBuckets(False)           
        return

class BucketContent_CLI (S3Util.AWS_Bucket ):
    def CLI_ListAllKeys (self ):
        self._reinit_( )
        self.get_list_of_keys_in_bucket( )
        Util.printDirList (self.bucketContent)
        
    def CLI_ListKeysAsFSTree( self ):        

        self._reinit_( )
        self.get_keys_in_bucket_as_fstree( )

        dir_stack = []
        keys = []

        d = self.root_dir
        dir_stack.append( d )

        while (True):
            keys = d.children.keys( )
            keys.sort( )

            i = 0
            for k in keys:
                entry = d.get_child(k)
                if (entry.__name__ == 'my_dir'):
                    print '[%d]\t<DIR>\t\t%s' % (i, entry.name)
                elif (entry.__name__ == 'my_file'):
                    print '[%d]\t\t%d\t%s' % (i, int(entry.size), entry.name)
                else: 
                    raise Failed()
                i = i + 1

            (action, ui) = Util.getInput( [['u', 'UP'], ['q', 'QUIT']], 0, i-1)

            if not action:
                entry = d.children[ keys[ui] ]
                if (entry.__name__ == 'my_dir'):
                    dir_stack.append( d )
                    d = entry
                elif (entry.__name__ == 'my_file'):                    
                    self.keyName = entry.s3_key
                    break
                else:
                    raise Failed()
            elif (action == 'QUIT'):
                break
            elif (action == 'UP'):
                d = dir_stack.pop( )
            else:
                raise Failed()

        return

    def CLI_GetObject( self ):
        self.CLI_ListKeysAsFSTree( )
        fileName = self.keyName
        if (fileName == ''):
            return

        prompt = 'Enter file Name [%s]-->' % fileName
        ui = Util.getStringInput ( prompt)
        if (ui != ''):
            fileName = ui

        key = AWS_Key ( self.CONN, self.bucketName, self.keyName)
        key.get_object_to_file ( fileName )
        return

    def CLI_PutObject( self ):        

        fileName = Util.getStringInput ('Enter local file Name-->' )
        self.keyName  = fileName

        prompt = 'Enter key Name. [%s]-->' % fileName
        ui = Util.getStringInput ( prompt )
        if (ui  != ''):
            self.keyName = ui

        key = AWS_Key( self.CONN, self.bucketName, self.keyName )
        key.put_object_from_file ( fileName )        

        return

    def CLI_DeleteObject ( self ):

        self.CLI_ListKeysAsFSTree( )
        if (self.keyName == ''):
            return

        key = AWS_Key ( self.CONN, self.bucketName, self.keyName )
        key.delete_object ( )            
        return

    def CLI_UploadDir( self ):
        
        root_dir = Util.getStringInput('Enter full path of the root directory to upload-->')        
        self.upload_dir ( root_dir )
        return

    def CLI_SyncUploadDir(self):
        root_dir = Util.getStringInput('Enter full path of the root directory to Sync & Upload-->')
        self.sync_dir( )
        return

    def CLI_DownloadDir ( self):
        root_dir = Util.getStringInput('Enter full path of the root directory where you want to download backup to-->')
        self.download_dir ( root_dir )
        return
    
                         
def printMenu( header, menu ):

    print '-' * 80
    print ' ' * ( (80 - len (header)) /2), header
    print '-' * 80
    
    index = 0
    for item in menu:
        print '(',index,') ', item[0]
        index = index + 1

    return ( Util.getDigitInput ( 0, index-1) )
                    
def ExitMenu( ):
    return "Exit"

def bucketMenu( ):
        
    aws_s3 = Bucket_CLI( AWSConnectionObject  = conn )
        
    userSelection = aws_s3.CLI_ListBuckets ( fGetInput = True )
    bucketName = aws_s3.get_bucket_name ( userSelection )
    
    BucketContent = BucketContent_CLI ( AWSConnectionObject = conn , bucketName=bucketName )
    
    hdr = 'BUCKET MENU [%s]' % bucketName
    
    while True:
        BucketMenu = [ ['List all contents of bucket', BucketContent.CLI_ListAllKeys], \
                       ['Display contents of bucket as FS Tree', BucketContent.CLI_ListKeysAsFSTree], \
                       ['Get Object from bucket', BucketContent.CLI_GetObject],
                       ['Put Object from bucket', BucketContent.CLI_PutObject],
                       ['Delete Object from bucket', BucketContent.CLI_DeleteObject],
                       ['Upload local directory', BucketContent.CLI_UploadDir],
                       ['SYNC Upload local directory', BucketContent.CLI_SyncUploadDir],
                       ['Download bucket to local directory', BucketContent.CLI_DownloadDir],
                       ['Exit', ExitMenu] ]
        

        action = printMenu( header=hdr, menu=BucketMenu )
        if ( BucketMenu[action][1]() == "Exit"):
            break
            
def mainMenu( ):

    global conn
            
    conn = S3Util.S3.AWSAuthConnection(Config.AWSProperties['AccessKey'], \
                                Config.AWSProperties['SecretKey'], ( Config.AWSProperties['SecureComm'] == True) )

    AllBuckets = Bucket_CLI ( AWSConnectionObject = conn )
                
    while True:
        MainMenu = [ ['List buckets', AllBuckets.CLI_ListBuckets], \
                     ['Add bucket', AllBuckets.CLI_AddBucket],\
                     ['Delete bucket', AllBuckets.CLI_DeleteBucket], \
                     ['Delete bucket and all contents', AllBuckets.CLI_DestroyBucket], \
                     ['BUCKET MENU', bucketMenu],
                     ['Exit', ExitMenu] ]
            
        action = printMenu ( header='Main Menu', menu=MainMenu )
        if ( MainMenu[action][1]( ) == "Exit" ):
            break
        
    sys.exit(0)
 
if __name__ == '__main__':
    mainMenu() 
