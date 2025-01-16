
export LDFLAGS="-L$VIRTUAL_ENV/lib -Wl,-rpath=$VIRTUAL_ENV/lib"
export CXXFLAGS="-I$VIRTUAL_ENV/include"

python3 -m pip install -U "git+https://git@github.com/Small-Bodies-Node/sbsearch.git#egg=sbsearch"
python3 -m pip install -U "git+https://git@github.com/Small-Bodies-Node/catch.git#egg=catch"
python3 -m pip install -U "git+https://git@github.com/Small-Bodies-Node/sbn_survey_image_service.git#egg=catch"
python3 -m pip install -U "git+https://git@github.com/Small-Bodies-Node/catch-sis-harvester.git#egg=catch-sis-harvester"
