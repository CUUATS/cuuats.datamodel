# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup

version = '0.2.0'

setup(name='cuuats.datamodel',
      version=version,
      description='A lightweight data access layer for ArcGIS',
      long_description='\n'.join([open(f).read() for f in [
          'README.rst',
          'HISTORY.rst'
      ]]),
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Win32 (MS Windows)',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'Topic :: Database',
          'Topic :: Scientific/Engineering :: GIS',
          'Programming Language :: Python :: 2.7',
      ],
      keywords='ArcGIS GIS data geodatabase',
      author='Matt Yoder',
      author_email='myoder@ccrpc.org',
      url='https://cuuats.org/',
      download_url='https://github.com/CUUATS/cuuats.datamodel/tarball/0.2.0',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['cuuats']
      )
