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

from optparse import OptionParser
import sys
import string
import logging.config
import awspylib.aws_genutilities as Util

doc=""" 
%prog [Options] -i inputfile [-o outputfile]
"""

LOGCONFIG_FILENAME = 'AWSPyLib/config/logconfig.ini'
S3CONFIG_FILENAME  = 'AWSPyLib/config/AWS.properties'

class AWSConfig:
    
    def __init__( self ):
        self.AWSProperties = {}
        self.get_configs( )

    def process_line(self, line):
        doc="""
        This function processes a line from a configuration file
        Lines that are not blank and do not start with a "#" are parsed
        for a name:value pair
        """        
        lines = []
        
            
        # Get rid of trailing blanks and new lines
        result = line.rstrip()
        result = result.rstrip('\n')
        
        #Skip blank lines and those starting with "#"
        if result == "" or result[:1] == "#":
            pass
        else:
            lines.append(result)    
            kvpair = result.split(':')        
            self.AWSProperties[kvpair[0]] = kvpair[1]
        
        return
    
    def get_configs(self):
        doc="""
        This function set's up a parser, processes command line arguments
        and reads the configuration from the configuration file specified 
        as a cmd line argument
        """
        
        log_level = [0, 10, 20, 30, 40, 50]
        parser = OptionParser(version="%prog 0.1", usage=doc)
        parser.add_option("-i", "--input", action="store", type="string", dest="inputfile", default="",
                          help="input file. Default is aws-s3.properties in PYTHONPATH")
        parser.add_option("-c", "--logconfig", action="store", type="string", dest="logconfigfile", default="",
                          help="log configuration file.  Default is s3_logconfig.ini in PTHONPATH")
        
        (ConfigOptions, args) = parser.parse_args()

        input = None
        if ConfigOptions.inputfile:
            input = open (ConfigOptions.inputfile, 'r' )
        else:
            input_file_name = Util.find_file(S3CONFIG_FILENAME)
            if (input_file_name == None):
                print ('Could not find input configuration file (%s) in PYTHONPATH' % S3CONFIG_FILENAME)
                sys.exit(0)
            else:
                input = open (input_file_name)

        log_config_file = Util.find_file (LOGCONFIG_FILENAME)
        if (log_config_file == None):
            print ('Could not find log configuration file (%s) in PYTHONPATH' % LOGCONFIG_FILENAME)
            sys.exit(0)
        else:
            logging.config.fileConfig(log_config_file)
                    
        logging.debug ( 'options:%s' % ConfigOptions )
        logging.debug( 'args:%s' % args)
            
        nLines = 0
        for line in input:
            self.process_line(line)
            nLines += 1
                            
        logging.debug( 'AWSProperties: %s' % self.AWSProperties)
                
        input.close()
        
        return

Config = AWSConfig( )

if __name__ == '__main__':
    # Test function
    pass
