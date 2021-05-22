from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="ormlib",
    version="${version}",
    author="Glance InMobi Pte.",
    author_email="cardpress-dev@glance.com",
    description="ORM Library Built on SQLAlchemy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.tools.inmobi.com/data-sciences/ormlib",
    packages=find_packages(include=["ormlib"]),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "SQLAlchemy>=1.4.0",
    ],
)
