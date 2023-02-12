import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

PROJECT_NAME = "gcpx"
USERNAME = "Hassi34"

setuptools.setup(
    name=f"{PROJECT_NAME}",
    include_package_data = True,
    version="0.0.0,dev1",
    license='MIT',
    author= "Hasanain Mehmood",
    author_email="hasanain@aicaliber.com",
    description="A python package for interacting with Google Cloud Platform (GCP)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=f"https://github.com/{USERNAME}/{PROJECT_NAME}",
    project_urls={
        "Bug Tracker": f"https://github.com/{USERNAME}/{PROJECT_NAME}/issues",
    },
    classifiers=[
        #'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
        #'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'Operating System :: OS Independent'
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.7",
    install_requires = [
        "google-cloud-storage",
        "google-cloud-bigquery",
        "tensorflow>=2.6.0,<2.11.0",
        "pandas"
    ]
)   