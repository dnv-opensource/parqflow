from setuptools import setup, find_packages

setup(name='parqflow',
      version='0.0.3',
      packages=find_packages(),
      description='CFD ParqFlow Format Library',
      long_description='File format describing CFD wind flow simulation variables',
      author='Diogo Friggo',
      author_email='diogo.friggo@dnv.com',
      maintainer='Diogo Friggo',
      maintainer_email='diogo.friggo@dnv.com',
      url='https://github.com/cfd_file_format',
      license='MIT',
      keywords=['wind', 'flow', 'cfd'],
      #https://pypi.org/pypi?%3Aaction=list_classifiers
      classifiers=['Development Status :: 1 - Planning',
                   'Intended Audience :: Customer Service',
                   'Environment :: Other Environment',
                   'Operating System :: Microsoft :: Windows',
                   'Programming Language :: Python',
                   'License :: OSI Approved :: MIT License',
      ],
      python_requires='>=3.7',
      install_requires=[
        'pandas',
        'pyarrow',
      ]
)
