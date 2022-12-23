from setuptools import find_packages, setup

setup(
    name="dagster_pipelines",
    packages=find_packages(exclude=["dagster_pipelines_tests"]),
    install_requires=[
        "dagster",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
