from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="yfinance-scraper",
    version="0.1.0",
    author="Trading Data Team",
    author_email="tradingdata@example.com",
    description="A package for retrieving and storing Yahoo Finance data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/yfinance-scraper",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "yfinance>=0.2.0",
        "pandas>=1.3.0",
        "pyarrow>=7.0.0",
        "numpy>=1.20.0",
        "requests>=2.26.0",
    ],
    entry_points={
        "console_scripts": [
            "yfinance-scraper=yfinance_scraper.cli:main",
        ],
    },
    scripts=[
        "scripts/save_combined_tickers.py",
    ],
    include_package_data=True,
) 