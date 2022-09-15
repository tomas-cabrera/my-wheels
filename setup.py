import setuptools
  
with open("README.md", "r") as fh:
    description = fh.read()
  
setuptools.setup(
    name="mywheels",
    version="0.0.1",
    author="tomas-cabrera",
    author_email="tcabrera@andrew.cmu.edu",
    packages=["mywheels"],
    description="The wheels I've built, so I don't have to reinvent them.",
    long_description=description,
    long_description_content_type="text/markdown",
    url="https://github.com/tomas-cabrera/my-wheels",
    license='MIT',
    python_requires='>=3.8',
    install_requires=[]
)
