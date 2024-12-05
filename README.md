CATCH-SIS Harvester
===================

Metadata harvesting for the [CATCH](https://github.com/Small-Bodies-Node/catch) and [SBN Survey Image Service](https://github.com/Small-Bodies-Node/sbn-survey-image-service) tools.


Periodic harvesting of accumulating data sets
---------------------------------------------

Create a working directory:
```
mkdir daily-harvest
cd daily-harvest
```

Checkout the repository, but just the daily-harvest scripts:
```
git clone --no-checkout https://github.com/Small-Bodies-Node/catch-sis-harvester.git src
cd daily-harvest
git sparse-checkout init --cone  # checkout root dir files (build, etc.)
git sparse-checkout set daily-harvest
git checkout
```

Create the virtual environment:
```
bash daily-harvest/_build_venv
```
