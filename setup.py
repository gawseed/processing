import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gawseed-processing",
    version="0.9.1",
    author="Wes Hardaker and USC/ISI",
    author_email="opensource@hardakers.net",
    description="A data processing engine for statistically analyzing time series data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gawseed/processing",
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'featureCounter.py = gawseed.scripts.featureCounter:main',
            'aggregator.py = gawseed.scripts.aggregator:main',
            'relationshipAnalysis.py = gawseed.scripts.relationshipAnalysis:main',
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires = '>=3.0',
    test_suite='nose.collector',
    tests_require=['nose'],
)
