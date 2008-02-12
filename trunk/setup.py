try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

__version__ = '0.5'

setup(name = "awspylib",
      version = __version__,
      description = "AWS Python Library",
      long_description="Python Library to Amazon Web Services",
      author = "Kiran Bhageshpur",
      author_email = "kiran_bhageshpur@hotmail.com",
      url = "http://code.google.com/p/awspylib/",
      packages = [ 'awspylib', 'awspylib.aws_s3', 'awspylib.test'],
      package_dir={'awspylib': 'awspylib'},
      package_data={'awspylib': ['config/*', 'docs/*']},
      license = 'MIT',
      platforms = 'Posix; Windows',
      )
