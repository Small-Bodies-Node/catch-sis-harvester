from . import atlas
from ...registry import register

register("atlas", atlas.add_arguments)
