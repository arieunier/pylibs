import setuptools


setuptools.setup(
    name="dclibs", # Replace with your own username
    version="0.0.1",
    author="Augustin Rieunier",
    author_email="arieunier@salesforce.com",
    description="Libraries used as part of the distributed compute framework",
    long_description="Libraries used as part of the distributed compute framework",
    long_description_content_type="text/markdown",
    url="https://github.com/arieunier/pylibs",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)