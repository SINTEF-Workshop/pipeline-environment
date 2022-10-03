from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="test_dagster",
        packages=find_packages(exclude=["test_dagster_tests"]),
        install_requires=[
            "dagster",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )
