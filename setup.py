from setuptools import find_packages, setup

LONG_DESC = open("README.rst", encoding="utf-8").read()

setup(
    name="asyncakumuli",
    use_scm_version={"version_scheme": "guess-next-dev", "local_scheme": "dirty-tag"},
    setup_requires=["setuptools_scm"],
    description="Write data to Akumuli",
    url="http://github.com/smurfix/asyncakumuli",
    long_description=LONG_DESC,
    author="Matthias Urlichs",
    author_email="matthias@urlichs.de",
    license="GPLv3 or later",
    packages=find_packages(),
    tests_require=["pytest-trio"],
    install_requires=["anyio >= 3", "pytz"],
    keywords=["iot", "logging"],
    python_requires=">=3.6",
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Framework :: Trio",
    ],
)
