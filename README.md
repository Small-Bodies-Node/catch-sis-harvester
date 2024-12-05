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
cd src
git sparse-checkout init --cone  # checkout root dir files (build, etc.)
git sparse-checkout set daily-harvest
git checkout
cp daily-harvest/* ..
cd ..
```

Create the virtual environment:
```
bash _build_venv
```

Optionally, clean up:
```
rm _build_venv s2geometry-v0.10-py-cmakelists.patch
rm -rf build src
```

Edit daily-harvest.sh to suit.  Create catch.config file.

Example crontab entry:
```
13 */6 * * * cd /elatus3/catch/apis-v3/daily-harvest && /bin/bash daily-harvest.sh
```
