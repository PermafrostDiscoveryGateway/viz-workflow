import sys

from osgeo.gdal import deprecation_warn
# import osgeo_utils.ogrmerge as a convenience to use as a script
from osgeo_utils.ogrmerge import *  # noqa
from osgeo_utils.ogrmerge import main

# docs: https://gdal.org/programs/ogrmerge.html
# source code (useful options like --merge and --append) https://github.com/OSGeo/gdal/blob/35c07b18316b4b6d238f6d60b82c31e25662ad27/swig/python/gdal-utils/osgeo_utils/ogrmerge.py
# Bonus: gdal-compare (deep comparison): https://github.com/OSGeo/gdal/blob/35c07b18316b4b6d238f6d60b82c31e25662ad27/swig/python/gdal-utils/osgeo_utils/gdalcompare.py

deprecation_warn('ogrmerge')
sys.exit(main(sys.argv))

