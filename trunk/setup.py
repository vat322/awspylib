try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

__version__ = '0.5'

setup(name = "AWSPyLib",
      version = __version__,
      description = "AWS Python Library",
      long_description="Python Library to Amazon Web Services",
      author = "Kiran Bhageshpur",
      author_email = "kiran_bhageshpur@hotmail.com",
      url = "http://vitsystems.unfuddle.com",
      packages = [ 'AWSPyLib', 'AWSPyLib.AWS_S3', 'AWSPyLib.test'],
      package_dir={'AWSPyLib': 'AWSPyLib'},
      package_data={'AWSPyLib': ['config/*', 'docs/*', 'test/test.txt']},
      license = 'MIT',
      platforms = 'Posix; Windows',
      )
