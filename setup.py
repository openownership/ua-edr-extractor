from setuptools import setup

setup(
    name='ua_edr_extractor',
    version='0.1',
    description='Parser and beneficial ownership extractor for the Ukrainian EDR register',
    url='http://github.com/openownership/ua-edr-extractor',
    author='Dmitry Chaplinksy, OpenOwnership',
    author_email='tech@openownership.org',
    license='MIT',
    packages=['ua_edr_extractor'],
    install_requires=[
        'nltk==3.2.4',
        'tokenize-uk==0.2.0',
        'PyYAML==3.12',
        'werkzeug',
        'PTable',
        'translitua==1.2.4',
        'mitie'
    ],
    zip_safe=False,
    entry_points = {
        'console_scripts': ['ua-edr-extractor=ua_edr_extractor.evaluate:main'],
    },
    include_package_data=True
)
