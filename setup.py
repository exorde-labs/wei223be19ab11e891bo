from setuptools import find_packages, setup

setup(
    name="wei223be19ab11e891bo",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "python-dotenv",
        "selenium==4.2.0",
        "exorde_data",
        "aiohttp",
        "pathlib"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)