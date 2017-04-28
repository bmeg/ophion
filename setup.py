from distutils.core import setup
setup(
  name = 'ophion',
  package_dir = {"" : "client/python"},
  py_modules = ['ophion'],
  version = '0.1',
  description = 'A Graph Database client library',
  author = 'BMEG/OHSU',
  author_email = 'kellrott@gmail.com',
  url = 'https://github.com/bmeg/ophion',
  download_url = 'https://github.com/bmeg/ophion/archive/0.1.tar.gz',
  keywords = ['client', 'graph', 'database'], # arbitrary keywords
  classifiers = [],
)
